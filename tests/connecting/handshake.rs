use navy_nvim_rs::rpc::handler::Dummy as DummyHandler;

use navy_nvim_rs::create;
use tokio::process::Command;
use tokio::test as atest;

use super::common::*;

#[cfg(unix)]
use navy_nvim_rs::error::HandshakeError;

#[atest]
async fn successful_handshake() {
  let handler = DummyHandler::new();

  create::new_child_handshake_cmd(
    Command::new(nvim_path()).args(["-u", "NONE", "--embed"]),
    handler,
    "handshake_message",
  )
  .await
  .expect("Should launch correctly");
}

#[cfg(unix)]
#[atest]
async fn successful_handshake_with_extra_output() {
  let handler = DummyHandler::new();
  let nvim = nvim_path();

  create::new_child_handshake_cmd(
    Command::new("/bin/sh").args(&[
      "-c",
      &format!(
        "echo 'extra output';{} -u NONE --embed",
        nvim.to_string_lossy()
      ),
    ]),
    handler,
    "handshake_message",
  )
  .await
  .expect("Should launch correctly");
}

#[cfg(unix)]
#[atest]
async fn unsuccessful_handshake_with_wrong_output() {
  let handler = DummyHandler::new();

  // NOTE: This has to match the exact length of the message sent
  let expected_request_len = 46;

  // Make sure that the command is alive for long enough by reading the request
  // message from stdin with dd
  let res = create::new_child_handshake_cmd(
    Command::new("/bin/sh").args(&[
        "-c",
        &format!("echo 'wrong output';
                  timeout 5 dd bs=1 count={expected_request_len} > /dev/null 2>&1")]),
    handler,
    "handshake_message",
  )
  .await;

  match res {
    Err(err) => match *err {
      HandshakeError::UnexpectedResponse(output) => {
        assert_eq!(output, "wrong output\n");
      }
      _ => {
        panic!("Unexpected error returned {}", err);
      }
    },
    _ => panic!("No error returned"),
  }
}
