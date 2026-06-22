//! Examples on how to use [`nvim-rs`](crate).
//!
//! The code in question is in the `examples` directory of the project. The
//! files in `src/examples/` contain the documentation.
//!
//! # Contents
//!
//! ### `quitting`
//!
//! An example showing how to handle quitting in a plugin by catching a [`closed
//! channel`](crate::error::CallError::is_channel_closed).

pub mod quitting;
