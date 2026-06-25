//! Buffers, windows, tabpages of neovim
mod buffer;
mod tabpage;
mod window;

pub use buffer::Buffer;
pub use tabpage::Tabpage;
pub use window::Window;

/// A macro to implement trait for the [`exttypes`](crate::exttypes)
#[macro_export]
macro_rules! impl_exttype_traits {
    ($ext:ident) => {
        impl<H> PartialEq for $ext<H>
        where
            H: Handler,
        {
            fn eq(&self, other: &Self) -> bool {
                self.code_data == other.code_data && self.neovim == other.neovim
            }
        }
        impl<H> Eq for $ext<H> where H: Handler {}

        impl<H> Clone for $ext<H>
        where
            H: Handler,
        {
            fn clone(&self) -> Self {
                Self {
                    code_data: self.code_data.clone(),
                    neovim: self.neovim.clone(),
                }
            }
        }

        impl<H> IntoVal<Value> for &$ext<H>
        where
            H: Handler,
        {
            fn into_val(self) -> Value {
                self.code_data.clone()
            }
        }
    };
}
