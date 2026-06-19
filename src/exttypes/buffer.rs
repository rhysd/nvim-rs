use rmpv::Value;
use tokio::io::AsyncWrite;

use crate::{Neovim, impl_exttype_traits, rpc::model::IntoVal};
/// A struct representing a neovim buffer. It is specific to a
/// [`Neovim`](crate::neovim::Neovim) instance, and calling a method on it will
/// always use this instance.
pub struct Buffer<W>
where
  W: AsyncWrite + Send + Unpin + 'static,
{
  pub(crate) code_data: Value,
  pub(crate) neovim: Neovim<W>,
}

impl_exttype_traits!(Buffer);
