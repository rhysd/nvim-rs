//! Handling redraw notifications received from neovim
//!
//! The core of a UI client is defining and implementing the
//! [`Handler`].
use std::{marker::PhantomData, sync::Arc};

use tokio::io::AsyncWrite;

use crate::rpc::redraw::{RedrawDecodeResult, RedrawNotification};

/// The central functionality of a UI client.
pub trait Handler: Send + Sync + Clone + 'static {
    /// The type used for writing requests and notifications to Neovim.
    type Writer: AsyncWrite + Send + Unpin + 'static;

    /// Handling a `redraw` notification on the handler loop without allocating
    /// an owned `String` or `Vec<Value>` for the notification payload.
    fn handle_redraw(&self, _redraw: RedrawNotification<'_>) -> RedrawDecodeResult<()> {
        Ok(())
    }
}

/// The dummy handler ignores redraw notifications.
///
/// It can be used if a client only wants to send requests to Neovim and get
/// responses.
#[derive(Default)]
pub struct Dummy<Q>
where
    Q: AsyncWrite + Send + Sync + Unpin + 'static,
{
    q: Arc<PhantomData<Q>>,
}

impl<Q> Clone for Dummy<Q>
where
    Q: AsyncWrite + Send + Sync + Unpin + 'static,
{
    fn clone(&self) -> Self {
        Dummy { q: self.q.clone() }
    }
}

impl<Q> Handler for Dummy<Q>
where
    Q: AsyncWrite + Send + Sync + Unpin + 'static,
{
    type Writer = Q;
}

impl<Q> Dummy<Q>
where
    Q: AsyncWrite + Send + Sync + Unpin + 'static,
{
    #[must_use]
    pub fn new() -> Dummy<Q> {
        Dummy {
            q: Arc::new(PhantomData),
        }
    }
}
