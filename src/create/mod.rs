//! Functions to spawn a [`neovim`](crate::neovim::Neovim) session.
//!
//! This implements various possibilities to connect to neovim, including
//! spawning an own child process. Available capabilities might depend on your
//! OS.
//!
//! API functions should be run from inside the tokio runtime.
pub mod tokio;

use core::future::Future;
use std::{fs::File, io};

use crate::rpc::handler::Handler;

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

/// Create a std::io::File for stdout, which is not line-buffered, as
/// opposed to std::io::Stdout.
#[cfg(unix)]
pub fn unbuffered_stdout() -> io::Result<File> {
  use std::{io::stdout, os::fd::AsFd};

  let owned_sout_fd = stdout().as_fd().try_clone_to_owned()?;
  Ok(File::from(owned_sout_fd))
}
#[cfg(windows)]
pub fn unbuffered_stdout() -> io::Result<File> {
  use std::{io::stdout, os::windows::io::AsHandle};

  let owned_sout_handle = stdout().as_handle().try_clone_to_owned()?;
  Ok(File::from(owned_sout_handle))
}
