use bytes::Bytes;
use criterion::{
  BatchSize, BenchmarkId, Criterion, Throughput, criterion_group,
  criterion_main,
};
use futures::{
  executor::block_on,
  io::{Cursor, sink},
  lock::Mutex,
};
use navy_nvim_rs::rpc::{
  model::{
    DecodeState, EncodeState, MessageType, RpcMessage,
    encode_single_string_arg_msg_to_state, encode_sync, encode_to_state,
  },
  redraw::{
    RedrawDecodeError, RedrawDecodeResult, RedrawFrame, RedrawFrameInfo,
    RedrawNotification,
  },
};
use rmpv::{Value, decode::read_value};
use std::{collections::HashSet, hint::black_box};

const NVIM_UI_FIXTURE: &[u8] =
  include_bytes!("fixtures/nvim_ui_notifications.bin");
const NVIM_UI_SCROLL_FIXTURE: &[u8] =
  include_bytes!("fixtures/nvim_ui_scroll_notifications.bin");
const FIXTURE_MAGIC: &[u8] = b"NVIMRSUI1\n";

#[derive(Clone)]
struct CapturedMessage {
  bytes: Bytes,
  event_names: Vec<String>,
}

#[derive(Clone)]
struct BenchInput {
  name: String,
  bytes: Bytes,
}

fn encode_request_message() -> RpcMessage {
  RpcMessage::RpcRequest {
    msgid: 1,
    method: "nvim_buf_get_lines".to_owned(),
    params: vec![
      Value::from(0),
      Value::from(0),
      Value::from(-1),
      Value::from(false),
    ],
  }
}

fn encode_message(msg: RpcMessage) -> Vec<u8> {
  let mut bytes = Vec::new();
  encode_sync(&mut bytes, msg).unwrap();
  bytes
}

fn non_redraw_rpc_batch(count: usize) -> Vec<u8> {
  let mut bytes = Vec::new();

  for index in 0..count {
    let msg = if index % 2 == 0 {
      RpcMessage::RpcResponse {
        msgid: index as u64,
        error: Value::Nil,
        result: Value::from(index as u64),
      }
    } else {
      RpcMessage::RpcNotification {
        method: "nvim_buf_lines_event".to_owned(),
        params: vec![
          Value::from(1),
          Value::from(0),
          Value::from(1),
          Value::from(vec![Value::from("line")]),
          Value::from(false),
        ],
      }
    };

    bytes.extend_from_slice(&encode_message(msg));
  }

  bytes
}

fn consume_redraw_arrays_for_bench(
  mut redraw: RedrawNotification<'_>,
) -> RedrawDecodeResult<usize> {
  let mut value_count = 0;

  redraw.for_each_batch(|batch| {
    black_box(batch.name);
    while !batch.args.is_empty() {
      batch.args.read_array(|args| {
        while !args.is_empty() {
          args.skip_next()?;
          value_count += 1;
          black_box(value_count);
        }

        Ok(())
      })?;
    }
    Ok(true)
  })?;

  Ok(value_count)
}

fn redraw_frame(bytes: Bytes) -> RedrawFrame {
  RedrawFrame::from_bytes(bytes).unwrap()
}

fn parse_redraw_arrays(bytes: &Bytes) -> usize {
  let frame = redraw_frame(bytes.clone());
  consume_redraw_arrays_for_bench(frame.notification().unwrap()).unwrap()
}

fn parse_redraw_arrays_batch(bytes: &Bytes, count: usize) -> usize {
  let mut rest = bytes.clone();
  let mut parsed = 0;
  let mut value_count = 0;

  while parsed < count {
    let info = RedrawFrameInfo::probe(&rest)
      .unwrap()
      .expect("redraw frame");
    let consumed = info.consumed();
    let frame = info.frame(rest.slice(..consumed));
    value_count +=
      consume_redraw_arrays_for_bench(frame.notification().unwrap()).unwrap();
    rest = rest.slice(consumed..);
    parsed += 1;
  }

  value_count
}

fn decode_redraw_frames_from_reader(
  decoder: &mut DecodeState,
  bytes: Vec<u8>,
  count: usize,
) -> usize {
  let mut reader = Cursor::new(bytes);

  block_on(async {
    let mut decoded = 0;
    let mut frame_bytes = 0;

    while decoded < count {
      while decoder.has_rest() {
        match RedrawFrameInfo::probe(decoder.rest()) {
          Ok(Some(info)) => {
            let bytes = decoder.take_rest(info.consumed());
            let frame = info.frame(bytes);
            frame_bytes += black_box(frame.as_bytes()).len();
          }
          Ok(None) => {
            if let Some(msg) = decoder.try_decode_message().unwrap() {
              black_box(msg);
            } else {
              break;
            }
          }
          Err(RedrawDecodeError::Incomplete) => break,
          Err(err) => panic!("redraw decode error: {err:?}"),
        }

        decoded += 1;
        if decoded == count {
          return frame_bytes;
        }
      }

      decoder.read_next_chunk(&mut reader).await.unwrap();
    }

    frame_bytes
  })
}

fn decode_non_redraw_messages_from_reader(
  decoder: &mut DecodeState,
  bytes: Vec<u8>,
  count: usize,
) -> usize {
  let mut reader = Cursor::new(bytes);

  block_on(async {
    let mut decoded = 0;

    while decoded < count {
      while decoder.has_rest() {
        match RedrawFrameInfo::probe(decoder.rest()) {
          Ok(Some(_)) => panic!("unexpected redraw frame"),
          Ok(None) => {
            if let Some(msg) = decoder.try_decode_message().unwrap() {
              black_box(msg);
              decoded += 1;
              if decoded == count {
                return decoded;
              }
            } else {
              break;
            }
          }
          Err(RedrawDecodeError::Incomplete) => break,
          Err(err) => panic!("redraw decode error: {err:?}"),
        }
      }

      decoder.read_next_chunk(&mut reader).await.unwrap();
    }

    decoded
  })
}

fn read_u32(input: &mut &[u8]) -> u32 {
  let (bytes, rest) = input.split_at(std::mem::size_of::<u32>());
  *input = rest;
  u32::from_le_bytes(bytes.try_into().unwrap())
}

fn redraw_event_names(bytes: &[u8]) -> Vec<String> {
  let mut input = bytes;
  let value = read_value(&mut input).unwrap();
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

fn nvim_ui_fixture(fixture: &[u8]) -> Vec<CapturedMessage> {
  let mut input = fixture;
  let (magic, rest) = input.split_at(FIXTURE_MAGIC.len());
  assert_eq!(magic, FIXTURE_MAGIC);
  input = rest;

  let count = read_u32(&mut input) as usize;
  let mut messages = Vec::with_capacity(count);

  for _ in 0..count {
    let len = read_u32(&mut input) as usize;
    let (bytes, rest) = input.split_at(len);
    input = rest;
    messages.push(CapturedMessage {
      bytes: Bytes::copy_from_slice(bytes),
      event_names: redraw_event_names(bytes),
    });
  }

  messages
}

fn selected_ui_inputs(captured: &[CapturedMessage]) -> Vec<BenchInput> {
  let mut selected = Vec::new();
  let mut used = HashSet::new();

  let mut select_unused = |name: &str, want: &[&str]| {
    let index = captured.iter().enumerate().position(|(index, msg)| {
      !used.contains(&index)
        && msg
          .event_names
          .iter()
          .any(|name| want.contains(&name.as_str()))
    });
    if let Some(index) = index.filter(|&i| used.insert(i)) {
      selected.push(BenchInput {
        name: name.to_string(),
        bytes: captured[index].bytes.clone(),
      });
    }
  };

  select_unused("nvim_ui_default_colors_set", &["default_colors_set"]);
  select_unused("nvim_ui_grid_resize", &["grid_resize"]);
  select_unused("nvim_ui_grid_line", &["grid_line"]);
  select_unused("nvim_ui_message", &["msg_show", "cmdline_show"]);
  select_unused("show_message", &["msg_show", "cmdline_show"]);

  selected
}

fn bench_encode(c: &mut Criterion) {
  let request_msg = encode_request_message();
  let mut group = c.benchmark_group("rpc/encode");

  group.bench_function("request", |b| {
    let state = Mutex::new(EncodeState::new(sink()));
    b.iter_batched(
      || request_msg.clone(),
      |msg| black_box(block_on(encode_to_state(&state, msg)).unwrap()),
      BatchSize::SmallInput,
    );
  });

  group.bench_function("nvim_input_ctrl_d", |b| {
    let state = Mutex::new(EncodeState::new(sink()));
    b.iter(|| {
      block_on(encode_single_string_arg_msg_to_state(
        &state,
        MessageType::Request(1),
        "nvim_input",
        "<C-D>",
      ))
      .unwrap()
    });
  });

  group.bench_function("nvim_input_ctrl_d_notify", |b| {
    let state = Mutex::new(EncodeState::new(sink()));
    b.iter(|| {
      block_on(encode_single_string_arg_msg_to_state(
        &state,
        MessageType::Notification,
        "nvim_input",
        "<C-D>",
      ))
      .unwrap()
    });
  });

  group.finish();
}

fn bench_decode(c: &mut Criterion) {
  let captured_ui_init = nvim_ui_fixture(NVIM_UI_FIXTURE);
  let captured_scroll_ui = nvim_ui_fixture(NVIM_UI_SCROLL_FIXTURE);

  let mut group = c.benchmark_group("rpc/decode");

  for input in selected_ui_inputs(&captured_ui_init) {
    group.throughput(Throughput::Bytes(input.bytes.len() as u64));
    group.bench_with_input(
      BenchmarkId::new("single_nvim_ui_init", &input.name),
      &input.bytes,
      |b, bytes| {
        let mut decoder = DecodeState::new();
        b.iter_batched(
          || bytes.to_vec(),
          |bytes| {
            black_box(decode_redraw_frames_from_reader(&mut decoder, bytes, 1))
          },
          BatchSize::SmallInput,
        );
      },
    );
  }

  let ui_batch_count = captured_ui_init.len();
  let ui_batch = captured_ui_init
    .iter()
    .flat_map(|msg| msg.bytes.iter().copied())
    .collect::<Vec<_>>();
  group.throughput(Throughput::Bytes(ui_batch.len() as u64));
  group.bench_function("batch_nvim_ui_init", |b| {
    let mut decoder = DecodeState::new();
    b.iter_batched(
      || ui_batch.clone(),
      |bytes| {
        black_box(decode_redraw_frames_from_reader(
          &mut decoder,
          bytes,
          ui_batch_count,
        ))
      },
      BatchSize::SmallInput,
    );
  });

  let scroll_ui_batch_count = captured_scroll_ui.len();
  let scroll_ui_batch = captured_scroll_ui
    .iter()
    .flat_map(|msg| msg.bytes.iter().copied())
    .collect::<Vec<_>>();
  group.throughput(Throughput::Bytes(scroll_ui_batch.len() as u64));
  group.bench_function("batch_nvim_ui_scroll", |b| {
    let mut decoder = DecodeState::new();
    b.iter_batched(
      || scroll_ui_batch.clone(),
      |bytes| {
        black_box(decode_redraw_frames_from_reader(
          &mut decoder,
          bytes,
          scroll_ui_batch_count,
        ))
      },
      BatchSize::SmallInput,
    );
  });

  const NON_REDRAW_RPC_BATCH_COUNT: usize = 64;
  let non_redraw_rpc_batch = non_redraw_rpc_batch(NON_REDRAW_RPC_BATCH_COUNT);
  group.throughput(Throughput::Bytes(non_redraw_rpc_batch.len() as u64));
  group.bench_function("non_redraw_rpc_batch", |b| {
    let mut decoder = DecodeState::new();
    b.iter_batched(
      || non_redraw_rpc_batch.clone(),
      |bytes| {
        black_box(decode_non_redraw_messages_from_reader(
          &mut decoder,
          bytes,
          NON_REDRAW_RPC_BATCH_COUNT,
        ))
      },
      BatchSize::SmallInput,
    );
  });

  group.finish();
}

fn bench_redraw_array_reader(c: &mut Criterion) {
  let captured_ui_init = nvim_ui_fixture(NVIM_UI_FIXTURE);
  let captured_scroll_ui = nvim_ui_fixture(NVIM_UI_SCROLL_FIXTURE);

  let mut group = c.benchmark_group("rpc/redraw_array_reader");

  for input in selected_ui_inputs(&captured_ui_init) {
    group.throughput(Throughput::Bytes(input.bytes.len() as u64));
    group.bench_with_input(
      BenchmarkId::new("single_nvim_ui_init", &input.name),
      &input.bytes,
      |b, bytes| b.iter(|| black_box(parse_redraw_arrays(bytes))),
    );
  }

  let ui_batch_count = captured_ui_init.len();
  let ui_batch = captured_ui_init
    .iter()
    .flat_map(|msg| msg.bytes.iter().copied())
    .collect::<Bytes>();
  group.throughput(Throughput::Bytes(ui_batch.len() as u64));
  group.bench_function("batch_nvim_ui_init", |b| {
    b.iter(|| black_box(parse_redraw_arrays_batch(&ui_batch, ui_batch_count)));
  });

  let scroll_ui_batch_count = captured_scroll_ui.len();
  let scroll_ui_batch = captured_scroll_ui
    .iter()
    .flat_map(|msg| msg.bytes.iter().copied())
    .collect::<Bytes>();
  group.throughput(Throughput::Bytes(scroll_ui_batch.len() as u64));
  group.bench_function("batch_nvim_ui_scroll", |b| {
    b.iter(|| {
      black_box(parse_redraw_arrays_batch(
        &scroll_ui_batch,
        scroll_ui_batch_count,
      ))
    });
  });

  group.finish();
}

fn rpc(c: &mut Criterion) {
  bench_encode(c);
  bench_decode(c);
  bench_redraw_array_reader(c);
}

criterion_group!(name = benches; config = Criterion::default().without_plots(); targets = rpc);
criterion_main!(benches);
