//! Some manually implemented API functions
use rmpv::{Value, ValueRef};

use crate::{
    Buffer, Tabpage, Window,
    error::CallError,
    neovim::Neovim,
    rpc::{handler::Handler, model::IntoVal, unpack::TryUnpack},
};

impl<H> Neovim<H>
where
    H: Handler,
{
    pub async fn list_bufs(&self) -> Result<Vec<Buffer<H>>, Box<CallError>> {
        match self.call("nvim_list_bufs", call_args![]).await?? {
            Value::Array(arr) => Ok(arr
                .into_iter()
                .map(|v| Buffer::new(v, self.clone()))
                .collect()),
            val => Err(CallError::WrongValueType(val))?,
        }
    }

    pub async fn get_current_buf(&self) -> Result<Buffer<H>, Box<CallError>> {
        Ok(self
            .call("nvim_get_current_buf", call_args![])
            .await?
            .map(|val| Buffer::new(val, self.clone()))?)
    }

    pub async fn list_wins(&self) -> Result<Vec<Window<H>>, Box<CallError>> {
        match self.call("nvim_list_wins", call_args![]).await?? {
            Value::Array(arr) => Ok(arr
                .into_iter()
                .map(|v| Window::new(v, self.clone()))
                .collect()),
            val => Err(CallError::WrongValueType(val))?,
        }
    }

    pub async fn get_current_win(&self) -> Result<Window<H>, Box<CallError>> {
        Ok(self
            .call("nvim_get_current_win", call_args![])
            .await?
            .map(|val| Window::new(val, self.clone()))?)
    }

    pub async fn create_buf(
        &self,
        listed: bool,
        scratch: bool,
    ) -> Result<Buffer<H>, Box<CallError>> {
        Ok(self
            .call("nvim_create_buf", call_args![listed, scratch])
            .await?
            .map(|val| Buffer::new(val, self.clone()))?)
    }

    pub async fn open_win(
        &self,
        buffer: &Buffer<H>,
        enter: bool,
        config: Vec<(Value, Value)>,
    ) -> Result<Window<H>, Box<CallError>> {
        Ok(self
            .call("nvim_open_win", call_args![buffer, enter, config])
            .await?
            .map(|val| Window::new(val, self.clone()))?)
    }

    pub async fn list_tabpages(&self) -> Result<Vec<Tabpage<H>>, Box<CallError>> {
        match self.call("nvim_list_tabpages", call_args![]).await?? {
            Value::Array(arr) => Ok(arr
                .into_iter()
                .map(|v| Tabpage::new(v, self.clone()))
                .collect()),
            val => Err(CallError::WrongValueType(val))?,
        }
    }

    pub async fn get_current_tabpage(&self) -> Result<Tabpage<H>, Box<CallError>> {
        Ok(self
            .call("nvim_get_current_tabpage", call_args![])
            .await?
            .map(|val| Tabpage::new(val, self.clone()))?)
    }

    pub async fn request_input(&self, keys: &str) -> Result<i64, Box<CallError>> {
        self.call_nvim_input(keys)
            .await??
            .try_unpack()
            .map_err(|v| Box::new(CallError::WrongValueType(v)))
    }

    #[inline]
    pub async fn notify_input(&self, keys: &str) -> Result<(), Box<CallError>> {
        self.notify_string("nvim_input", keys).await
    }

    #[inline]
    pub async fn out_write(&self, str: &str) -> Result<(), Box<CallError>> {
        self.notify_string("nvim_out_write", str).await
    }

    #[inline]
    pub async fn err_write(&self, str: &str) -> Result<(), Box<CallError>> {
        self.notify_string("nvim_err_write", str).await
    }

    #[inline]
    pub async fn err_writeln(&self, str: &str) -> Result<(), Box<CallError>> {
        self.notify_string("nvim_err_writeln", str).await
    }

    #[inline]
    pub async fn ui_set_focus(&self, gained: bool) -> Result<(), Box<CallError>> {
        let args = [ValueRef::Boolean(gained)];
        self.notify_value_ref("nvim_ui_set_focus", &args).await
    }

    #[inline]
    pub async fn ui_try_resize(&self, width: i64, height: i64) -> Result<(), Box<CallError>> {
        let args = [ValueRef::from(width), ValueRef::from(height)];
        self.notify_value_ref("nvim_ui_try_resize", &args).await
    }

    pub async fn cmd_value_ref(
        &self,
        cmd: ValueRef<'_>,
        opts: ValueRef<'_>,
    ) -> Result<String, Box<CallError>> {
        self.call_value_ref("nvim_cmd", &[cmd, opts])
            .await??
            .try_unpack()
            .map_err(|v| Box::new(CallError::WrongValueType(v)))
    }
}
