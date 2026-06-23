use rmpv::Value;

use crate::{
    Neovim, impl_exttype_traits,
    rpc::{encode::IntoVal, handler::Handler},
};
/// A struct representing a neovim buffer. It is specific to a
/// [`Neovim`](crate::neovim::Neovim) instance, and calling a method on it will
/// always use this instance.
pub struct Buffer<H>
where
    H: Handler,
{
    pub(crate) code_data: Value,
    pub(crate) neovim: Neovim<H>,
}

impl_exttype_traits!(Buffer);
