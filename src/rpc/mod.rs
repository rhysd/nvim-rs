//! RPC functionality for [`neovim`](crate::neovim::Neovim)
//!
//! For most plugins, the main implementation work will consist of defining and
//! implementing the [`handler`](crate::rpc::handler::Handler).
pub mod decode;
pub mod encode;
pub mod handler;
pub mod redraw;
mod skip;
pub mod unpack;

pub use self::{
    decode::RpcResponse,
    encode::{IntoVal, RpcMessage},
};
pub use rmpv::{Value, ValueRef};
