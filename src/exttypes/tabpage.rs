use rmpv::Value;

use crate::{
    Neovim,
    error::CallError,
    exttypes::Window,
    impl_exttype_traits,
    rpc::{handler::Handler, model::IntoVal},
};

/// A struct representing a neovim tabpage. It is specific to a
/// [`Neovim`](crate::neovim::Neovim) instance, and calling a method on it will
/// always use this instance.
pub struct Tabpage<H>
where
    H: Handler,
{
    pub(crate) code_data: Value,
    pub(crate) neovim: Neovim<H>,
}

impl_exttype_traits!(Tabpage);

impl<H> Tabpage<H>
where
    H: Handler,
{
    /// since: 1
    pub async fn list_wins(&self) -> Result<Vec<Window<H>>, Box<CallError>> {
        match self
            .neovim
            .call("nvim_tabpage_list_wins", call_args![self.code_data.clone()])
            .await??
        {
            Value::Array(arr) => Ok(arr
                .into_iter()
                .map(|v| Window::new(v, self.neovim.clone()))
                .collect()),
            val => Err(CallError::WrongValueType(val))?,
        }
    }
    /// since: 1
    pub async fn get_win(&self) -> Result<Window<H>, Box<CallError>> {
        Ok(self
            .neovim
            .call("nvim_tabpage_get_win", call_args![self.code_data.clone()])
            .await?
            .map(|val| Window::new(val, self.neovim.clone()))?)
    }
}
