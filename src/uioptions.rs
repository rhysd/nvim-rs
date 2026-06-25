//! Options for UI implementations
//!
//! This should be used with the manually implemented
//! [`ui_attach`](crate::neovim::Neovim::ui_attach)
use rmpv::ValueRef;

#[derive(Default)]
pub struct UiAttachOptions<'a> {
    rgb: Option<bool>,
    r#override: Option<bool>,
    ext_cmdline: Option<bool>,
    ext_hlstate: Option<bool>,
    ext_linegrid: Option<bool>,
    ext_messages: Option<bool>,
    ext_multigrid: Option<bool>,
    ext_popupmenu: Option<bool>,
    ext_tabline: Option<bool>,
    ext_termcolors: Option<bool>,
    term_name: Option<&'a str>,
    term_colors: Option<u64>,
    term_background: Option<&'a str>,
    stdin_fd: Option<u64>,
    stdin_tty: Option<bool>,
    stdout_tty: Option<bool>,
    ext_wildmenu: Option<bool>,
}

impl<'a> UiAttachOptions<'a> {
    pub fn set_rgb(&mut self, val: bool) {
        self.rgb = Some(val);
    }

    pub fn set_override(&mut self, val: bool) {
        self.r#override = Some(val);
    }

    pub fn set_cmdline_external(&mut self, val: bool) {
        self.ext_cmdline = Some(val);
    }

    pub fn set_hlstate_external(&mut self, val: bool) {
        self.ext_hlstate = Some(val);
    }

    pub fn set_linegrid_external(&mut self, val: bool) {
        self.ext_linegrid = Some(val);
    }

    pub fn set_messages_external(&mut self, val: bool) {
        self.ext_messages = Some(val);
    }

    pub fn set_multigrid_external(&mut self, val: bool) {
        self.ext_multigrid = Some(val);
    }

    pub fn set_popupmenu_external(&mut self, val: bool) {
        self.ext_popupmenu = Some(val);
    }

    pub fn set_tabline_external(&mut self, val: bool) {
        self.ext_tabline = Some(val);
    }

    pub fn set_termcolors_external(&mut self, val: bool) {
        self.ext_termcolors = Some(val);
    }

    pub fn set_term_name(&mut self, val: &'a str) {
        self.term_name = Some(val);
    }

    pub fn set_term_colors(&mut self, val: u64) {
        self.term_colors = Some(val);
    }

    pub fn set_term_background(&mut self, val: &'a str) {
        self.term_background = Some(val);
    }

    pub fn set_stdin_fd(&mut self, val: u64) {
        self.stdin_fd = Some(val);
    }

    pub fn set_stdin_tty(&mut self, val: bool) {
        self.stdin_tty = Some(val);
    }

    pub fn set_stdout_tty(&mut self, val: bool) {
        self.stdout_tty = Some(val);
    }

    pub fn set_wildmenu_external(&mut self, val: bool) {
        self.ext_wildmenu = Some(val);
    }

    #[must_use]
    pub(crate) fn to_value_map(&self) -> ValueRef<'a> {
        let mut map = vec![];

        if let Some(value) = self.rgb {
            map.push(("rgb".into(), ValueRef::Boolean(value)));
        }
        if let Some(value) = self.r#override {
            map.push(("override".into(), ValueRef::Boolean(value)));
        }
        if let Some(value) = self.ext_cmdline {
            map.push(("ext_cmdline".into(), ValueRef::Boolean(value)));
        }
        if let Some(value) = self.ext_hlstate {
            map.push(("ext_hlstate".into(), ValueRef::Boolean(value)));
        }
        if let Some(value) = self.ext_linegrid {
            map.push(("ext_linegrid".into(), ValueRef::Boolean(value)));
        }
        if let Some(value) = self.ext_messages {
            map.push(("ext_messages".into(), ValueRef::Boolean(value)));
        }
        if let Some(value) = self.ext_multigrid {
            map.push(("ext_multigrid".into(), ValueRef::Boolean(value)));
        }
        if let Some(value) = self.ext_popupmenu {
            map.push(("ext_popupmenu".into(), ValueRef::Boolean(value)));
        }
        if let Some(value) = self.ext_tabline {
            map.push(("ext_tabline".into(), ValueRef::Boolean(value)));
        }
        if let Some(value) = self.ext_termcolors {
            map.push(("ext_termcolors".into(), ValueRef::Boolean(value)));
        }
        if let Some(value) = self.term_name {
            map.push(("term_name".into(), value.into()));
        }
        if let Some(value) = self.term_colors {
            map.push(("term_colors".into(), value.into()));
        }
        if let Some(value) = self.term_background {
            map.push(("term_background".into(), value.into()));
        }
        if let Some(value) = self.stdin_fd {
            map.push(("stdin_fd".into(), value.into()));
        }
        if let Some(value) = self.stdin_tty {
            map.push(("stdin_tty".into(), ValueRef::Boolean(value)));
        }
        if let Some(value) = self.stdout_tty {
            map.push(("stdout_tty".into(), ValueRef::Boolean(value)));
        }
        if let Some(value) = self.ext_wildmenu {
            map.push(("ext_wildmenu".into(), ValueRef::Boolean(value)));
        }

        ValueRef::Map(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_encode_empty_map() {
        let options = UiAttachOptions::default();

        assert_eq!(ValueRef::Map(vec![]), options.to_value_map());
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

        let value_map = options.to_value_map();

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

    #[test]
    fn setters_overwrite_existing_options() {
        let mut options = UiAttachOptions::default();
        options.set_rgb(true);
        options.set_rgb(false);
        options.set_term_name("first");
        options.set_term_name("second");
        options.set_stdin_fd(1);
        options.set_stdin_fd(9);

        assert_eq!(
            ValueRef::Map(vec![
                ("rgb".into(), ValueRef::Boolean(false)),
                ("term_name".into(), "second".into()),
                ("stdin_fd".into(), 9_u64.into()),
            ]),
            options.to_value_map()
        );
    }
}
