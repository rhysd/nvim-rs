mod common;
use common::*;

use std::{fs, process::Command};

use tempfile::Builder;

fn viml_escape(in_str: &str) -> String {
    in_str.replace('\\', r"\\")
}

#[test]
fn basic() {
    let dir = Builder::new().prefix("nvim-rs.test").tempdir().unwrap();
    let dir_path = dir.path();
    let buf_path = dir_path.join("curbuf.txt");

    let c1 = format!(
        "let jobid = jobstart([\"{}\", \"{}\"], {{\"rpc\": v:true}})",
        viml_escape("target/debug/examples/basic"),
        viml_escape(buf_path.to_str().unwrap())
    );
    let c2 = format!(
        "if wait(5000, {{-> filereadable('{}')}}) < 0 | cquit | endif",
        viml_escape(buf_path.to_str().unwrap())
    );
    let c3 = r#"wqa!"#;

    let args = &[
        "-u",
        "NONE",
        "-i",
        "NONE",
        "--headless",
        "-c",
        &c1,
        "-c",
        &c2,
        "-c",
        c3,
    ];
    let status = Command::new(nvim_path()).args(args).status().unwrap();

    assert!(status.success());

    let buf = fs::read_to_string(buf_path).unwrap();

    assert_eq!("Ext(0, [1])", buf.trim());
}
