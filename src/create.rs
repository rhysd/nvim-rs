//! Functions to spawn a [`neovim`](crate::neovim::Neovim) session.
//!
//! This implements various possibilities to connect to neovim, including
//! spawning an own child process. Available capabilities might depend on your
//! OS.
//!
//! API functions should be run from inside the tokio runtime.
use std::{
    fs::File,
    io::{self, Error},
    path::Path,
    process::Stdio,
};

use tokio::{
    fs::File as TokioFile,
    io::{WriteHalf, split, stdin},
    net::{TcpStream, ToSocketAddrs},
    process::{Child, ChildStdin, Command},
    spawn,
    task::JoinHandle,
};

use crate::{error::LoopError, neovim::Neovim, rpc::handler::Handler};

#[cfg(unix)]
type Connection = tokio::net::UnixStream;
#[cfg(windows)]
type Connection = tokio::net::windows::named_pipe::NamedPipeClient;

type SpawnedChild<H> = (Neovim<H>, JoinHandle<Result<(), Box<LoopError>>>, Child);

/// Create a std::io::File for stdout, which is not line-buffered, as
/// opposed to std::io::Stdout.
#[cfg(unix)]
fn unbuffered_stdout() -> io::Result<File> {
    use std::{io::stdout, os::fd::AsFd};

    let owned_sout_fd = stdout().as_fd().try_clone_to_owned()?;
    Ok(File::from(owned_sout_fd))
}
#[cfg(windows)]
fn unbuffered_stdout() -> io::Result<File> {
    use std::{io::stdout, os::windows::io::AsHandle};

    let owned_sout_handle = stdout().as_handle().try_clone_to_owned()?;
    Ok(File::from(owned_sout_handle))
}

/// Connect to a neovim instance via tcp
pub async fn new_tcp<A, H>(
    addr: A,
    handler: H,
) -> io::Result<(Neovim<H>, JoinHandle<Result<(), Box<LoopError>>>)>
where
    H: Handler<Writer = WriteHalf<TcpStream>>,
    A: ToSocketAddrs,
{
    let stream = TcpStream::connect(addr).await?;
    let (reader, writer) = split(stream);
    let (neovim, io) = Neovim::new(reader, writer, handler);
    let io_handle = spawn(io);

    Ok((neovim, io_handle))
}

/// Connect to a neovim instance via unix socket (Unix) or named pipe (Windows)
pub async fn new_path<H, P: AsRef<Path> + Clone>(
    path: P,
    handler: H,
) -> io::Result<(Neovim<H>, JoinHandle<Result<(), Box<LoopError>>>)>
where
    H: Handler<Writer = WriteHalf<Connection>> + Send + 'static,
{
    let stream = {
        #[cfg(unix)]
        {
            use tokio::net::UnixStream;

            UnixStream::connect(path).await?
        }
        #[cfg(windows)]
        {
            use std::time::Duration;
            use tokio::net::windows::named_pipe::ClientOptions;
            use tokio::time;

            // From windows-sys so we don't have to depend on that for just this constant
            // https://docs.rs/windows-sys/latest/windows_sys/Win32/Foundation/constant.ERROR_PIPE_BUSY.html
            pub const ERROR_PIPE_BUSY: i32 = 231i32;

            // Based on the example in the tokio docs, see explanation there
            // https://docs.rs/tokio/latest/tokio/net/windows/named_pipe/struct.NamedPipeClient.html
            loop {
                match ClientOptions::new().open(path.as_ref()) {
                    Ok(client) => break client,
                    Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) => (),
                    Err(e) => return Err(e),
                }

                time::sleep(Duration::from_millis(50)).await;
            }
        }
    };
    let (reader, writer) = split(stream);
    let (neovim, io) = Neovim::new(reader, writer, handler);
    let io_handle = spawn(io);

    Ok((neovim, io_handle))
}

/// Connect to a neovim instance by spawning a new one
pub async fn new_child<H>(handler: H) -> io::Result<SpawnedChild<H>>
where
    H: Handler<Writer = ChildStdin> + Send + 'static,
{
    if cfg!(target_os = "windows") {
        new_child_path("nvim.exe", handler).await
    } else {
        new_child_path("nvim", handler).await
    }
}

/// Connect to a neovim instance by spawning a new one
pub async fn new_child_path<H, S: AsRef<Path>>(
    program: S,
    handler: H,
) -> io::Result<SpawnedChild<H>>
where
    H: Handler<Writer = ChildStdin> + Send + 'static,
{
    let mut cmd = Command::new(program.as_ref());
    cmd.arg("--embed");
    new_child_cmd(cmd, handler)
}

/// Connect to a neovim instance by spawning a new one
///
/// stdin/stdout will be rewritten to `Stdio::piped()`
pub fn new_child_cmd<H>(mut cmd: Command, handler: H) -> io::Result<SpawnedChild<H>>
where
    H: Handler<Writer = ChildStdin> + Send + 'static,
{
    let mut child = cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::other("Can't open stdout"))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| Error::other("Can't open stdin"))?;

    let (neovim, io) = Neovim::new(stdout, stdin, handler);
    let io_handle = spawn(io);

    Ok((neovim, io_handle, child))
}

/// Connect to the neovim instance that spawned this process over stdin/stdout
pub async fn new_parent<H>(
    handler: H,
) -> Result<(Neovim<H>, JoinHandle<Result<(), Box<LoopError>>>), Error>
where
    H: Handler<Writer = tokio::fs::File>,
{
    let sout = TokioFile::from_std(unbuffered_stdout()?);

    let (neovim, io) = Neovim::new(stdin(), sout, handler);
    let io_handle = spawn(io);

    Ok((neovim, io_handle))
}

#[cfg(test)]
mod tests {
    use std::{env, path::PathBuf, time::Duration};

    use tokio::{
        sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        time,
    };

    use super::*;
    use crate::{
        rpc::{
            handler::Handler,
            redraw::{RedrawDecodeResult, RedrawNotification},
        },
        uioptions::UiAttachOptions,
    };

    #[derive(Debug)]
    enum RedrawSignal {
        Any,
        GridResize { cols: u64, rows: u64 },
    }

    #[derive(Clone)]
    struct TestHandler {
        redraw_tx: UnboundedSender<RedrawSignal>,
    }

    impl Handler for TestHandler {
        type Writer = ChildStdin;

        fn handle_redraw(&self, mut redraw: RedrawNotification<'_>) -> RedrawDecodeResult<()> {
            let _ = self.redraw_tx.send(RedrawSignal::Any);

            redraw.for_each_batch(|batch| {
                if batch.name == "grid_resize" {
                    while !batch.args.is_empty() {
                        batch.args.read_array(|args| {
                            let _grid = args.read_u64()?;
                            let cols = args.read_u64()?;
                            let rows = args.read_u64()?;
                            let _ = self.redraw_tx.send(RedrawSignal::GridResize { cols, rows });
                            Ok(())
                        })?;
                    }
                }
                Ok(true)
            })
        }
    }

    fn nvim_test_bin() -> PathBuf {
        let path = env::var_os("NVIMRS_TEST_BIN")
            .expect("NVIMRS_TEST_BIN must point to a valid nvim executable");
        let path = PathBuf::from(path);
        assert!(
            path.exists(),
            "nvim bin from NVIMRS_TEST_BIN does not exist: {}",
            path.display()
        );
        path
    }

    async fn recv_redraw(redraw_rx: &mut UnboundedReceiver<RedrawSignal>) -> RedrawSignal {
        time::timeout(Duration::from_secs(5), redraw_rx.recv())
            .await
            .expect("timed out waiting for redraw notification")
            .expect("redraw channel closed")
    }

    async fn wait_for_grid_resize(
        redraw_rx: &mut UnboundedReceiver<RedrawSignal>,
        expected_cols: u64,
        expected_rows: u64,
    ) {
        loop {
            match recv_redraw(redraw_rx).await {
                RedrawSignal::GridResize { cols, rows }
                    if cols == expected_cols && rows == expected_rows =>
                {
                    return;
                }
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn new_child_cmd_spawns_embedded_nvim_and_attaches_ui() {
        let path = nvim_test_bin();

        let mut cmd = Command::new(path);
        cmd.args(["--embed", "--headless", "--clean", "-i", "NONE"])
            .stderr(Stdio::piped());

        let (redraw_tx, mut redraw_rx) = unbounded_channel();
        let handler = TestHandler { redraw_tx };
        let (nvim, io_handle, mut child) = new_child_cmd(cmd, handler).unwrap();

        let mut options = UiAttachOptions::default();
        options.set_rgb(true);
        options.set_linegrid_external(true);
        options.set_hlstate_external(true);
        time::timeout(Duration::from_secs(5), nvim.ui_attach(80, 24, options))
            .await
            .expect("timed out waiting for nvim_ui_attach response")
            .unwrap();
        let _ = recv_redraw(&mut redraw_rx).await;

        nvim.ui_try_resize(100, 30).await.unwrap();
        wait_for_grid_resize(&mut redraw_rx, 100, 30).await;

        let quit_input = "<Cmd>qall!<CR>";
        let written = time::timeout(Duration::from_secs(5), nvim.request_input(quit_input))
            .await
            .expect("timed out waiting for nvim_input response")
            .unwrap();
        assert_eq!(written, quit_input.len() as i64);

        let status = match time::timeout(Duration::from_secs(5), child.wait()).await {
            Ok(status) => status.unwrap(),
            Err(err) => {
                child.kill().await.unwrap();
                panic!("timed out waiting for nvim process to exit: {err}");
            }
        };
        assert!(status.success(), "nvim exited with status: {status}");
        io_handle.abort();
    }
}
