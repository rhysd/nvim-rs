use rmpv::{decode::read_value, encode::write_value, Value};
use std::{
  env, fs,
  io::{self, Read, Write},
  path::{Path, PathBuf},
  process::{Command, Stdio},
};

const FIXTURE_MAGIC: &[u8] = b"NVIMRSUI1\n";

struct RecordingReader<R> {
  inner: R,
  bytes: Vec<u8>,
}

impl<R> RecordingReader<R> {
  fn new(inner: R) -> Self {
    Self {
      inner,
      bytes: Vec::new(),
    }
  }

  fn read_value(&mut self) -> io::Result<(Value, Vec<u8>)>
  where
    R: Read,
  {
    self.bytes.clear();
    let value = read_value(self)
      .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    Ok((value, std::mem::take(&mut self.bytes)))
  }
}

impl<R: Read> Read for RecordingReader<R> {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    let n = self.inner.read(buf)?;
    self.bytes.extend_from_slice(&buf[..n]);
    Ok(n)
  }
}

struct CapturedMessage {
  bytes: Vec<u8>,
  event_names: Vec<String>,
}

fn request(
  writer: &mut impl Write,
  msgid: i64,
  method: &str,
  params: Vec<Value>,
) -> io::Result<()> {
  let msg = Value::Array(vec![
    Value::from(0),
    Value::from(msgid),
    Value::from(method),
    Value::Array(params),
  ]);
  write_value(writer, &msg)
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
  writer.flush()
}

fn response_id(value: &Value) -> Option<i64> {
  let Value::Array(items) = value else {
    return None;
  };

  if items.len() == 4 && items[0].as_i64() == Some(1) {
    items[1].as_i64()
  } else {
    None
  }
}

fn is_redraw_notification(value: &Value) -> bool {
  let Value::Array(items) = value else {
    return false;
  };

  items.len() == 3
    && items[0].as_i64() == Some(2)
    && items[1].as_str() == Some("redraw")
}

fn redraw_event_names(value: &Value) -> Vec<String> {
  let Value::Array(items) = value else {
    return Vec::new();
  };
  let Some(Value::Array(events)) = items.get(2) else {
    return Vec::new();
  };

  events
    .iter()
    .filter_map(|event| {
      let Value::Array(event_items) = event else {
        return None;
      };
      event_items.first()?.as_str().map(str::to_owned)
    })
    .collect()
}

fn read_until_response<R: Read>(
  reader: &mut RecordingReader<R>,
  msgid: i64,
  captured: &mut Vec<CapturedMessage>,
) -> io::Result<()> {
  for _ in 0..512 {
    let (value, bytes) = reader.read_value()?;

    if is_redraw_notification(&value) {
      captured.push(CapturedMessage {
        bytes,
        event_names: redraw_event_names(&value),
      });
    }

    if response_id(&value) == Some(msgid) {
      return Ok(());
    }
  }

  Err(io::Error::new(
    io::ErrorKind::TimedOut,
    format!("no response for msgid {msgid}"),
  ))
}

fn ui_options() -> Value {
  Value::Map(vec![
    (Value::from("rgb"), Value::from(true)),
    (Value::from("ext_linegrid"), Value::from(true)),
    (Value::from("ext_multigrid"), Value::from(true)),
    (Value::from("ext_cmdline"), Value::from(true)),
    (Value::from("ext_messages"), Value::from(true)),
    (Value::from("ext_popupmenu"), Value::from(true)),
    (Value::from("ext_tabline"), Value::from(true)),
  ])
}

fn nvim_path() -> PathBuf {
  if let Ok(path) = env::var("NVIMRS_TEST_BIN") {
    return PathBuf::from(path);
  }
  PathBuf::from("nvim")
}

fn capture_nvim_ui_notifications() -> io::Result<Vec<CapturedMessage>> {
  let mut child = Command::new(nvim_path())
    .args(["-u", "NONE", "--embed", "--headless"])
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::null())
    .spawn()?;

  let mut stdin = child.stdin.take().ok_or_else(|| {
    io::Error::new(io::ErrorKind::BrokenPipe, "missing stdin")
  })?;
  let stdout = child.stdout.take().ok_or_else(|| {
    io::Error::new(io::ErrorKind::BrokenPipe, "missing stdout")
  })?;
  let mut reader = RecordingReader::new(stdout);
  let mut captured = Vec::new();
  let mut msgid = 1;

  request(
    &mut stdin,
    msgid,
    "nvim_ui_attach",
    vec![Value::from(80), Value::from(24), ui_options()],
  )?;
  read_until_response(&mut reader, msgid, &mut captured)?;

  let lines = (0..80)
    .map(|i| {
      Value::from(format!("line {i}: abcdefghijklmnopqrstuvwxyz0123456789"))
    })
    .collect::<Vec<_>>();

  msgid += 1;
  request(
    &mut stdin,
    msgid,
    "nvim_buf_set_lines",
    vec![
      Value::from(0),
      Value::from(0),
      Value::from(-1),
      Value::from(false),
      Value::Array(lines),
    ],
  )?;
  read_until_response(&mut reader, msgid, &mut captured)?;

  for command in [
    "set laststatus=2",
    "redraw",
    "echo 'nvim-rs decode benchmark'",
    "vsplit | split | tabnew | enew",
    "redraw",
  ] {
    msgid += 1;
    request(
      &mut stdin,
      msgid,
      "nvim_command",
      vec![Value::from(command)],
    )?;
    read_until_response(&mut reader, msgid, &mut captured)?;
  }

  msgid += 1;
  request(&mut stdin, msgid, "nvim_eval", vec![Value::from("1")])?;
  read_until_response(&mut reader, msgid, &mut captured)?;

  let _ = request(
    &mut stdin,
    msgid + 1,
    "nvim_command",
    vec![Value::from("qa!")],
  );
  drop(stdin);
  let _ = child.kill();
  let _ = child.wait();

  Ok(captured)
}

fn write_u32(output: &mut Vec<u8>, value: usize) {
  output.extend_from_slice(&(value as u32).to_le_bytes());
}

fn write_fixture(path: &Path, captured: &[CapturedMessage]) -> io::Result<()> {
  let mut output = Vec::new();
  output.extend_from_slice(FIXTURE_MAGIC);
  write_u32(&mut output, captured.len());

  for msg in captured {
    write_u32(&mut output, msg.bytes.len());
    output.extend_from_slice(&msg.bytes);
  }

  if let Some(parent) = path.parent() {
    fs::create_dir_all(parent)?;
  }
  fs::write(path, output)
}

fn output_path() -> PathBuf {
  env::args_os().nth(1).map(PathBuf::from).unwrap_or_else(|| {
    PathBuf::from("benches")
      .join("fixtures")
      .join("nvim_ui_notifications.bin")
  })
}

fn main() -> io::Result<()> {
  let output = output_path();
  let captured = capture_nvim_ui_notifications()?;
  write_fixture(&output, &captured)?;

  let total_bytes: usize = captured.iter().map(|msg| msg.bytes.len()).sum();
  println!(
    "wrote {} nvim UI notifications ({} bytes) to {}",
    captured.len(),
    total_bytes,
    output.display()
  );

  for (index, msg) in captured.iter().enumerate() {
    println!(
      "{index:>2}: {:>6} bytes {:?}",
      msg.bytes.len(),
      msg.event_names
    );
  }

  Ok(())
}
