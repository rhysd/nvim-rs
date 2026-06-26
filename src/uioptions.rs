//! Options for UI implementations
//!
//! This should be used with the manually implemented
//! [`ui_attach`](crate::neovim::Neovim::ui_attach)
use rmpv::ValueRef;

#[derive(Default)]
pub struct UiAttachOptions<'a>(Vec<(ValueRef<'a>, ValueRef<'a>)>);

impl<'a> UiAttachOptions<'a> {
    pub fn set_rgb(&mut self, val: bool) {
        self.0.push(("rgb".into(), ValueRef::Boolean(val)));
    }

    pub fn set_override(&mut self, val: bool) {
        self.0.push(("override".into(), ValueRef::Boolean(val)));
    }

    pub fn set_cmdline_external(&mut self, val: bool) {
        self.0.push(("ext_cmdline".into(), ValueRef::Boolean(val)));
    }

    pub fn set_hlstate_external(&mut self, val: bool) {
        self.0.push(("ext_hlstate".into(), ValueRef::Boolean(val)));
    }

    pub fn set_linegrid_external(&mut self, val: bool) {
        self.0.push(("ext_linegrid".into(), ValueRef::Boolean(val)));
    }

    pub fn set_messages_external(&mut self, val: bool) {
        self.0.push(("ext_messages".into(), ValueRef::Boolean(val)));
    }

    pub fn set_multigrid_external(&mut self, val: bool) {
        self.0
            .push(("ext_multigrid".into(), ValueRef::Boolean(val)));
    }

    pub fn set_popupmenu_external(&mut self, val: bool) {
        self.0
            .push(("ext_popupmenu".into(), ValueRef::Boolean(val)));
    }

    pub fn set_tabline_external(&mut self, val: bool) {
        self.0.push(("ext_tabline".into(), ValueRef::Boolean(val)));
    }

    pub fn set_termcolors_external(&mut self, val: bool) {
        self.0
            .push(("ext_termcolors".into(), ValueRef::Boolean(val)));
    }

    pub fn set_term_name(&mut self, val: &'a str) {
        self.0.push(("term_name".into(), val.into()));
    }

    pub fn set_term_colors(&mut self, val: u64) {
        self.0.push(("term_colors".into(), val.into()));
    }

    pub fn set_term_background(&mut self, val: &'a str) {
        self.0.push(("term_background".into(), val.into()));
    }

    pub fn set_stdin_fd(&mut self, val: u64) {
        self.0.push(("stdin_fd".into(), val.into()));
    }

    pub fn set_stdin_tty(&mut self, val: bool) {
        self.0.push(("stdin_tty".into(), ValueRef::Boolean(val)));
    }

    pub fn set_stdout_tty(&mut self, val: bool) {
        self.0.push(("stdout_tty".into(), ValueRef::Boolean(val)));
    }

    pub fn set_wildmenu_external(&mut self, val: bool) {
        self.0.push(("ext_wildmenu".into(), ValueRef::Boolean(val)));
    }

    #[must_use]
    pub(crate) fn into_value(self) -> ValueRef<'a> {
        debug_assert_eq!(
            self.0
                .iter()
                .find(|(key, _)| self.0.iter().filter(|(k, _)| key == k).count() != 1),
            None,
            "duplicate entry in UI options: {:?}",
            self.0,
        );
        ValueRef::Map(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_encode_empty_map() {
        let options = UiAttachOptions::default();

        assert_eq!(ValueRef::Map(vec![]), options.into_value());
    }

    #[test]
    fn setters_encode_all_ui_attach_options() {
        let mut options = UiAttachOptions::default();
        options.set_rgb(true);
        options.set_override(false);
        options.set_cmdline_external(true);
        options.set_hlstate_external(false);
        options.set_linegrid_external(true);
        options.set_messages_external(false);
        options.set_multigrid_external(true);
        options.set_popupmenu_external(true);
        options.set_tabline_external(false);
        options.set_termcolors_external(true);
        options.set_term_name("xterm-256color");
        options.set_term_colors(256);
        options.set_term_background("dark");
        options.set_stdin_fd(3);
        options.set_stdin_tty(true);
        options.set_stdout_tty(false);
        options.set_wildmenu_external(true);

        let value_map = options.into_value();

        assert_eq!(
            ValueRef::Map(vec![
                ("rgb".into(), ValueRef::Boolean(true)),
                ("override".into(), ValueRef::Boolean(false)),
                ("ext_cmdline".into(), ValueRef::Boolean(true)),
                ("ext_hlstate".into(), ValueRef::Boolean(false)),
                ("ext_linegrid".into(), ValueRef::Boolean(true)),
                ("ext_messages".into(), ValueRef::Boolean(false)),
                ("ext_multigrid".into(), ValueRef::Boolean(true)),
                ("ext_popupmenu".into(), ValueRef::Boolean(true)),
                ("ext_tabline".into(), ValueRef::Boolean(false)),
                ("ext_termcolors".into(), ValueRef::Boolean(true)),
                ("term_name".into(), "xterm-256color".into()),
                ("term_colors".into(), 256_u64.into()),
                ("term_background".into(), "dark".into()),
                ("stdin_fd".into(), 3_u64.into()),
                ("stdin_tty".into(), ValueRef::Boolean(true)),
                ("stdout_tty".into(), ValueRef::Boolean(false)),
                ("ext_wildmenu".into(), ValueRef::Boolean(true)),
            ]),
            value_map
        );
    }
}
