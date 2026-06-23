use rmpv::Value;

use super::{Buffer, Tabpage};
use crate::{
    Neovim,
    error::CallError,
    impl_exttype_traits,
    rpc::{encode::IntoVal, handler::Handler},
};

/// A struct representing a neovim window. It is specific to a
/// [`Neovim`](crate::neovim::Neovim) instance, and calling a method on it will
/// always use this instance.
pub struct Window<H>
where
    H: Handler,
{
    pub(crate) code_data: Value,
    pub(crate) neovim: Neovim<H>,
}

impl_exttype_traits!(Window);

impl<H> Window<H>
where
    H: Handler,
{
    /// since: 1
    pub async fn get_buf(&self) -> Result<Buffer<H>, Box<CallError>> {
        Ok(self
            .neovim
            .call("nvim_win_get_buf", call_args![self.code_data.clone()])
            .await?
            .map(|val| Buffer::new(val, self.neovim.clone()))?)
    }
    /// since: 1
    pub async fn get_tabpage(&self) -> Result<Tabpage<H>, Box<CallError>> {
        Ok(self
            .neovim
            .call("nvim_win_get_tabpage", call_args![self.code_data.clone()])
            .await?
            .map(|val| Tabpage::new(val, self.neovim.clone()))?)
    }
}
