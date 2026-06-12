use criterion::{
  BatchSize, BenchmarkId, Criterion, Throughput, criterion_group,
  criterion_main,
};
use futures::{
  executor::block_on,
  io::{Cursor, sink},
  lock::Mutex,
};
use navy_nvim_rs::rpc::model::{
  DecodeState, EncodeState, RpcMessage, encode_nvim_input_with_state,
  encode_with_state,
};
use rmpv::{Value, decode::read_value};
use std::{collections::HashSet, hint::black_box, sync::Arc};

const NVIM_UI_FIXTURE: &[u8] =
  include_bytes!("fixtures/nvim_ui_notifications.bin");
const NVIM_UI_SCROLL_FIXTURE: &[u8] =
  include_bytes!("fixtures/nvim_ui_scroll_notifications.bin");
const FIXTURE_MAGIC: &[u8] = b"NVIMRSUI1\n";

#[derive(Clone)]
struct CapturedMessage {
  bytes: Vec<u8>,
  event_names: Vec<String>,
}

#[derive(Clone)]
struct BenchInput {
  name: String,
  bytes: Vec<u8>,
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

fn decode_one_from_reader_with_state(
  decoder: &mut DecodeState,
  bytes: Vec<u8>,
) -> RpcMessage {
  let mut reader = Cursor::new(bytes);
  block_on(decoder.decode(&mut reader)).unwrap()
}

fn decode_many_from_reader_with_state(
  decoder: &mut DecodeState,
  bytes: Vec<u8>,
  count: usize,
) -> usize {
  let mut reader = Cursor::new(bytes);

  for _ in 0..count {
    let msg = block_on(decoder.decode(&mut reader)).unwrap();
    black_box(msg);
  }
  count
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
      bytes: bytes.to_vec(),
      event_names: redraw_event_names(bytes),
    });
  }

  messages
}

fn selected_ui_inputs(captured: &[CapturedMessage]) -> Vec<BenchInput> {
  let mut selected = Vec::new();
  let mut used = HashSet::new();

  push_ui_input(
    &mut selected,
    &mut used,
    captured,
    "nvim_ui_initial_redraw",
    (!captured.is_empty()).then_some(0),
  );
  let index = first_unused_ui_input(captured, &used, |msg| {
    msg.event_names.iter().any(|event| event == "grid_resize")
  });
  push_ui_input(
    &mut selected,
    &mut used,
    captured,
    "nvim_ui_grid_resize",
    index,
  );

  let index = first_unused_ui_input(captured, &used, |msg| {
    msg.event_names.iter().any(|event| event == "grid_line")
  });
  push_ui_input(
    &mut selected,
    &mut used,
    captured,
    "nvim_ui_grid_line",
    index,
  );

  let index = first_unused_ui_input(captured, &used, |msg| {
    msg
      .event_names
      .iter()
      .any(|event| event == "msg_show" || event == "cmdline_show")
  });
  push_ui_input(&mut selected, &mut used, captured, "nvim_ui_message", index);

  let index = largest_unused_ui_input(captured, &used);
  push_ui_input(
    &mut selected,
    &mut used,
    captured,
    "nvim_ui_largest_redraw",
    index,
  );

  selected
}

fn push_ui_input(
  selected: &mut Vec<BenchInput>,
  used: &mut HashSet<usize>,
  captured: &[CapturedMessage],
  name: &str,
  index: Option<usize>,
) {
  let Some(index) = index else {
    return;
  };
  if used.insert(index) {
    selected.push(BenchInput {
      name: name.to_owned(),
      bytes: captured[index].bytes.clone(),
    });
  }
}

fn first_unused_ui_input(
  captured: &[CapturedMessage],
  used: &HashSet<usize>,
  pred: impl Fn(&CapturedMessage) -> bool,
) -> Option<usize> {
  captured
    .iter()
    .enumerate()
    .position(|(index, msg)| !used.contains(&index) && pred(msg))
}

fn largest_unused_ui_input(
  captured: &[CapturedMessage],
  used: &HashSet<usize>,
) -> Option<usize> {
  captured
    .iter()
    .enumerate()
    .filter(|(index, _)| !used.contains(index))
    .max_by_key(|(_, msg)| msg.bytes.len())
    .map(|(index, _)| index)
}

fn bench_encode(c: &mut Criterion) {
  let request_msg = encode_request_message();
  let mut group = c.benchmark_group("rpc/encode");

  group.bench_function("request", |b| {
    let state = Arc::new(Mutex::new(EncodeState::new(sink())));
    b.iter_batched(
      || request_msg.clone(),
      |msg| black_box(block_on(encode_with_state(state.clone(), msg)).unwrap()),
      BatchSize::SmallInput,
    );
  });

  group.bench_function("nvim_input_ctrl_d", |b| {
    let state = Arc::new(Mutex::new(EncodeState::new(sink())));
    b.iter(|| {
      black_box(
        block_on(encode_nvim_input_with_state(
          state.clone(),
          1,
          black_box("<C-D>"),
        ))
        .unwrap(),
      )
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
          || bytes.clone(),
          |bytes| {
            black_box(decode_one_from_reader_with_state(&mut decoder, bytes))
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
        black_box(decode_many_from_reader_with_state(
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
        black_box(decode_many_from_reader_with_state(
          &mut decoder,
          bytes,
          scroll_ui_batch_count,
        ))
      },
      BatchSize::SmallInput,
    );
  });

  group.finish();
}

fn rpc(c: &mut Criterion) {
  bench_encode(c);
  bench_decode(c);
}

criterion_group!(name = benches; config = Criterion::default().without_plots(); targets = rpc);
criterion_main!(benches);
