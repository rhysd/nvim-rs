use std::{
    env,
    error::Error,
    ffi::OsString,
    io::{self, Write},
    process::Stdio,
    time::Duration,
};

use navy_nvim_rs::{
    Handler, Neovim, UiAttachOptions, create,
    error::LoopError,
    rpc::redraw::{RedrawDecodeResult, RedrawNotification},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, ChildStdin, Command},
    time,
};

type AnyError = Box<dyn Error + Send + Sync>;

#[derive(Clone)]
struct DumpRedrawHandler;

impl Handler for DumpRedrawHandler {
    type Writer = ChildStdin;

    fn handle_redraw(&self, mut redraw: RedrawNotification<'_>) -> RedrawDecodeResult<()> {
        println!("redraw notification: {} batch(es)", redraw.batch_count());
        redraw.for_each_batch(|batch| {
            println!("  {}", batch.name);

            let mut i = 0usize;
            while !batch.args.is_empty() {
                let value = batch.args.read_value_ref()?;
                println!("    [{i}] {value:?}");
                i += 1;
            }

            Ok(true)
        })
    }
}

fn default_nvim_path() -> OsString {
    if cfg!(windows) {
        "nvim.exe".into()
    } else {
        "nvim".into()
    }
}

fn nvim_path() -> OsString {
    env::args_os()
        .nth(1)
        .or_else(|| env::var_os("NVIMRS_TEST_BIN"))
        .unwrap_or_else(default_nvim_path)
}

fn nvim_command() -> Command {
    let mut cmd = Command::new(nvim_path());
    cmd.args(["--embed", "--headless", "--clean", "-i", "NONE"])
        .stderr(Stdio::inherit());
    cmd
}

fn print_prompt() -> io::Result<()> {
    eprint!("nvim> ");
    io::stderr().flush()
}

async fn input_repl(nvim: Neovim<DumpRedrawHandler>) -> Result<(), AnyError> {
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();

    loop {
        print_prompt()?;

        let Some(line) = lines.next_line().await? else {
            break;
        };

        if line.is_empty() {
            continue;
        }

        let input = format!("{line}<Enter>");
        nvim.notify_input(&input).await?;
    }

    Ok(())
}

fn handle_io_loop_result(result: Result<Result<(), Box<LoopError>>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(err)) if err.is_channel_closed() => {}
        Ok(Err(err)) => eprintln!("RPC IO loop failed: {err}"),
        Err(err) => eprintln!("RPC IO task failed: {err}"),
    }
}

async fn wait_or_kill_child(mut child: Child) -> Result<(), AnyError> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }

    if time::timeout(Duration::from_secs(2), child.wait())
        .await
        .is_err()
    {
        child.kill().await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    let (nvim, mut io_handle, child) = create::new_child_cmd(nvim_command(), DumpRedrawHandler)?;

    let mut opts = UiAttachOptions::default();
    opts.set_rgb(true);
    opts.set_linegrid_external(true);
    opts.set_hlstate_external(true);
    nvim.ui_attach(80, 24, opts).await?;

    eprintln!("Type Neovim input line by line. Example: :quit");
    let mut input_task = tokio::spawn(input_repl(nvim.clone()));

    tokio::select! {
        result = &mut io_handle => {
            input_task.abort();
            handle_io_loop_result(result);
        }
        result = &mut input_task => {
            match result {
                Ok(Ok(())) => {
                    let _ = nvim.notify_input("<Cmd>qall!<CR>").await;
                }
                Ok(Err(err)) => return Err(err),
                Err(err) => return Err(err.into()),
            }
            io_handle.abort();
        }
    }

    wait_or_kill_child(child).await
}
