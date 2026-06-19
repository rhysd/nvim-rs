//! Examples on how to use [`nvim-rs`](crate).
//!
//! The code in question is in the `examples` directory of the project. The
//! files in `src/examples/` contain the documentation.
//!
//! # Contents
//!
//! ### `handler_drop`
//!
//! An example showing how to implement cleanup-logic by implementing
//! [`Drop`](std::ops::Drop) for the [`handler`](crate::rpc::handler::Handler).
//!
//! ### `quitting`
//!
//! An example showing how to handle quitting in a plugin by catching a [`closed
//! channel`](crate::error::CallError::is_channel_closed).
//!
//!
//! ## `scorched_earth`
//!
//! A port of a real existing plugin.
//!
//! ## `bench_tokio`
//!
//! Some crude benchmarks to measure performance. After running
//!
//! ```sh
//! cargo build --examples --release
//! ```
//!
//! (the features aren't all compatible, so you need to run those separately
//! indeed) you can run `nvim -u bench_examples.vim`, and after so and so long
//! get a table in a modified buffer that tells you some numbers.
pub mod handler_drop;
pub mod quitting;
pub mod scorched_earth;
