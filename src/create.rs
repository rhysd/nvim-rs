//! Functions to spawn a [`neovim`](crate::neovim::Neovim) session.
//!
//! This implements various possibilities to connect to neovim, including
//! spawning an own child process. Available capabilities might depend on your
//! OS.
//!
//! API functions should be run from inside the tokio runtime.
use core::future::Future;
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

#[cfg(unix)]
type Connection = tokio::net::UnixStream;
#[cfg(windows)]
type Connection = tokio::net::windows::named_pipe::NamedPipeClient;

type SpawnedChild = (
    Neovim<ChildStdin>,
    JoinHandle<Result<(), Box<LoopError>>>,
    Child,
);

use crate::{
    error::{HandshakeError, LoopError},
    neovim::Neovim,
    rpc::handler::Handler,
};

/// A task to generalize spawning a future that returns `()`.
///
/// This is automatically implemented on your
/// [`Handler`](crate::rpc::handler::Handler) using the appropriate runtime.
///
/// If you have a runtime that brings appropriate types, you can implement this
/// on your [`Handler`](crate::rpc::handler::Handler) and use
/// [`Neovim::new`](crate::neovim::Neovim::new) to connect to neovim.
pub trait Spawner: Handler {
    type Handle;

    fn spawn<Fut>(&self, future: Fut) -> Self::Handle
    where
        Fut: Future<Output = ()> + Send + 'static;
}

impl<H> Spawner for H
where
    H: Handler,
{
    type Handle = JoinHandle<()>;

    fn spawn<Fut>(&self, future: Fut) -> Self::Handle
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        spawn(future)
    }
}

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
) -> io::Result<(
    Neovim<WriteHalf<TcpStream>>,
    JoinHandle<Result<(), Box<LoopError>>>,
)>
where
    H: Handler<Writer = WriteHalf<TcpStream>>,
    A: ToSocketAddrs,
{
    let stream = TcpStream::connect(addr).await?;
    let (reader, writer) = split(stream);
    let (neovim, io) = Neovim::<WriteHalf<TcpStream>>::new(reader, writer, handler);
    let io_handle = spawn(io);

    Ok((neovim, io_handle))
}

/// Connect to a neovim instance via unix socket (Unix) or named pipe (Windows)
pub async fn new_path<H, P: AsRef<Path> + Clone>(
    path: P,
    handler: H,
) -> io::Result<(
    Neovim<WriteHalf<Connection>>,
    JoinHandle<Result<(), Box<LoopError>>>,
)>
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
    let (neovim, io) = Neovim::<WriteHalf<Connection>>::new(reader, writer, handler);
    let io_handle = spawn(io);

    Ok((neovim, io_handle))
}

/// Connect to a neovim instance by spawning a new one
pub async fn new_child<H>(handler: H) -> io::Result<SpawnedChild>
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
pub async fn new_child_path<H, S: AsRef<Path>>(program: S, handler: H) -> io::Result<SpawnedChild>
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
pub fn new_child_cmd<H>(mut cmd: Command, handler: H) -> io::Result<SpawnedChild>
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

    let (neovim, io) = Neovim::<ChildStdin>::new(stdout, stdin, handler);
    let io_handle = spawn(io);

    Ok((neovim, io_handle, child))
}

/// Connect to the neovim instance that spawned this process over stdin/stdout
pub async fn new_parent<H>(
    handler: H,
) -> Result<
    (
        Neovim<tokio::fs::File>,
        JoinHandle<Result<(), Box<LoopError>>>,
    ),
    Error,
>
where
    H: Handler<Writer = tokio::fs::File>,
{
    let sout = TokioFile::from_std(unbuffered_stdout()?);

    let (neovim, io) = Neovim::<tokio::fs::File>::new(stdin(), sout, handler);
    let io_handle = spawn(io);

    Ok((neovim, io_handle))
}

/// Connect to a neovim instance by spawning a new one and send a handshake
/// message. Unlike `new_child_cmd`, this function is tolerant to extra
/// data in the reader before the handshake response is received.
///
/// `message` should be a unique string that is normally not found in the
/// stdout. Due to the way Neovim packs strings, the length has to be either
/// less than 20 characters or more than 31 characters long.
/// See https://github.com/neovim/neovim/issues/32784 for more information.
pub async fn new_child_handshake_cmd<H>(
    cmd: &mut Command,
    handler: H,
    message: &str,
) -> Result<SpawnedChild, Box<HandshakeError>>
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

    let (neovim, io) = Neovim::<ChildStdin>::handshake(stdout, stdin, handler, message).await?;
    let io_handle = spawn(io);

    Ok((neovim, io_handle, child))
}
