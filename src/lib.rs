//! # Rust library for Neovim clients
//!
//! Implements support for rust plugins for
//! [Neovim](https://github.com/neovim/neovim) through its msgpack-rpc API.
//!
//! ### Origins
//!
//! This library uses Rust's `async/await` to send requests and notifications to
//! Neovim and to receive redraw notifications from Neovim.
//!
//! ### Status
//!
//! As of the end of 2019, I'm somewhat confident to recommend starting to use
//! this library. The overall handling should not change anymore. A breaking
//! change I kind of expect is adding error variants to
//! [`CallError`](crate::error::CallError) when I start working on the API
//! (right now, it panics when messages don't have the right format, I'll want
//! to return proper errors in that case).
//!
//! I've not yet worked through the details of what-to-export, but I'm quite
//! willing to consider what people need or want.
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
extern crate rmp;
extern crate rmpv;

pub mod rpc;
#[macro_use]
pub mod neovim;
pub mod error;
pub mod exttypes;
pub mod neovim_api;
pub mod neovim_api_manual;
pub mod uioptions;

pub mod create;

pub use crate::{
    exttypes::{Buffer, Tabpage, Window},
    neovim::Neovim,
    rpc::handler::Handler,
    uioptions::UiAttachOptions,
};

pub use rmpv::Value;
