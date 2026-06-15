//! Options for UI implementations
//!
//! This should be used with the manually implemented
//! [`ui_attach`](crate::neovim::Neovim::ui_attach)
use rmpv::ValueRef;

pub enum UiOption {
  Rgb(bool),
  Override(bool),
  ExtCmdline(bool),
  ExtHlstate(bool),
  ExtLinegrid(bool),
  ExtMessages(bool),
  ExtMultigrid(bool),
  ExtPopupmenu(bool),
  ExtTabline(bool),
  ExtTermcolors(bool),
  TermName(String),
  TermColors(u64),
  TermBackground(String),
  StdinFd(u64),
  StdinTty(bool),
  StdoutTty(bool),
  ExtWildmenu(bool),
}

impl UiOption {
  fn to_value_pair(&self) -> (ValueRef<'_>, ValueRef<'_>) {
    (self.name().into(), self.value())
  }

  fn name(&self) -> &'static str {
    match self {
      Self::Rgb(_) => "rgb",
      Self::Override(_) => "override",
      Self::ExtCmdline(_) => "ext_cmdline",
      Self::ExtHlstate(_) => "ext_hlstate",
      Self::ExtLinegrid(_) => "ext_linegrid",
      Self::ExtMessages(_) => "ext_messages",
      Self::ExtMultigrid(_) => "ext_multigrid",
      Self::ExtPopupmenu(_) => "ext_popupmenu",
      Self::ExtTabline(_) => "ext_tabline",
      Self::ExtTermcolors(_) => "ext_termcolors",
      Self::TermName(_) => "term_name",
      Self::TermColors(_) => "term_colors",
      Self::TermBackground(_) => "term_background",
      Self::StdinFd(_) => "stdin_fd",
      Self::StdinTty(_) => "stdin_tty",
      Self::StdoutTty(_) => "stdout_tty",
      Self::ExtWildmenu(_) => "ext_wildmenu",
    }
  }

  fn value(&self) -> ValueRef<'_> {
    match self {
      Self::Rgb(val)
      | Self::Override(val)
      | Self::ExtCmdline(val)
      | Self::ExtHlstate(val)
      | Self::ExtLinegrid(val)
      | Self::ExtMessages(val)
      | Self::ExtMultigrid(val)
      | Self::ExtPopupmenu(val)
      | Self::ExtTabline(val)
      | Self::ExtTermcolors(val)
      | Self::StdinTty(val)
      | Self::StdoutTty(val)
      | Self::ExtWildmenu(val) => ValueRef::Boolean(*val),
      Self::TermName(val) | Self::TermBackground(val) => val.as_str().into(),
      Self::TermColors(val) | Self::StdinFd(val) => (*val).into(),
    }
  }
}

#[derive(Default)]
pub struct UiAttachOptions {
  options: Vec<(&'static str, UiOption)>,
}

macro_rules! ui_opt_setters {
  ($( $opt:ident as $set:ident($type:ty) );+ ;) => {
    impl UiAttachOptions {
      $(
        pub fn $set(&mut self, val: $type) -> &mut Self {
          self.set_option(UiOption::$opt(val.into()));
          self
        }
      )+
    }
  }
}

ui_opt_setters! (

  Rgb as set_rgb(bool);
  Override as set_override(bool);
  ExtCmdline as set_cmdline_external(bool);
  ExtHlstate as set_hlstate_external(bool);
  ExtLinegrid as set_linegrid_external(bool);
  ExtMessages as set_messages_external(bool);
  ExtMultigrid as set_multigrid_external(bool);
  ExtPopupmenu as set_popupmenu_external(bool);
  ExtTabline as set_tabline_external(bool);
  ExtTermcolors as set_termcolors_external(bool);
  TermName as set_term_name(&str);
  TermColors as set_term_colors(u64);
  TermBackground as set_term_background(&str);
  StdinFd as set_stdin_fd(u64);
  StdinTty as set_stdin_tty(bool);
  StdoutTty as set_stdout_tty(bool);
  ExtWildmenu as set_wildmenu_external(bool);
);

impl UiAttachOptions {
  #[must_use]
  pub fn new() -> UiAttachOptions {
    UiAttachOptions {
      options: Vec::new(),
    }
  }

  fn set_option(&mut self, option: UiOption) {
    let name = option.name();
    let position = self.options.iter().position(|o| o.0 == name);

    if let Some(position) = position {
      self.options[position].1 = option;
    } else {
      self.options.push((name, option));
    }
  }

  #[must_use]
  pub fn to_value_map(&self) -> ValueRef<'_> {
    let map = self.options.iter().map(|o| o.1.to_value_pair()).collect();
    ValueRef::Map(map)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_ui_options() {
    let mut options = UiAttachOptions::new();
    let value_map = options
      .set_rgb(true)
      .set_rgb(false)
      .set_popupmenu_external(true)
      .to_value_map();

    assert_eq!(
      ValueRef::Map(vec![
        ("rgb".into(), ValueRef::Boolean(false)),
        ("ext_popupmenu".into(), ValueRef::Boolean(true)),
      ]),
      value_map
    );
  }
}
