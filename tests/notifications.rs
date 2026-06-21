use std::{sync::Arc, time::Duration};

use tokio::{
    process::{ChildStdin, Command},
    sync::{
        Notify,
        mpsc::{UnboundedSender, unbounded_channel},
    },
    task::yield_now,
};

use navy_nvim_rs::{self, Value, create, neovim::Neovim};

mod common;
use common::*;

const ITERS: usize = 25;
const TIMEOUT: Duration = Duration::from_secs(60);

macro_rules! timeout {
    ($x:expr) => {
        async {
            tokio::time::timeout(TIMEOUT, $x)
                .await
                .expect(&format!("Timed out waiting for {}", stringify!($x)))
        }
    };
}

#[derive(Clone)]
struct Handler {
    result_sender: UnboundedSender<u8>,
    notifiers: Arc<Vec<Notify>>,
}

impl navy_nvim_rs::Handler for Handler {
    type Writer = ChildStdin;

    async fn handle_notify(&self, name: String, args: Vec<Value>, _: Neovim<Self::Writer>) {
        assert_eq!(name, "idx");
        let idx = args[0].as_i64().unwrap();

        /* Wait until we've received a message from the test, then send back the
         * number we received.
         */
        self.notifiers[idx as usize].notified().await;
        self.result_sender.send(idx as u8).unwrap();
    }
}

#[tokio::test]
async fn sequential_notifications() {
    // Create a tokio notifier for each nvim notification that we'll be handling
    let mut notifiers = Vec::<Notify>::with_capacity(ITERS);
    for _ in 0..ITERS {
        notifiers.push(Notify::new());
    }
    let notifiers = Arc::new(notifiers);

    /* Create a channel the notification handler will use each time for writing
     * back the number provided by each notification
     */
    let (result_sender, mut result_receiver) = unbounded_channel();
    let handler = Handler {
        result_sender,
        notifiers: notifiers.clone(),
    };

    // Startup nvim
    let mut cmd = Command::new(nvim_path());
    cmd.args(["-u", "NONE", "--embed", "--headless"]);
    let (nvim, _io_handler, _child) = create::new_child_cmd(cmd, handler).unwrap();

    // Generate the commands to send the notifications
    let mut nvim_cmds = Vec::<String>::with_capacity(ITERS);
    for i in 0..ITERS {
        nvim_cmds.push(format!("call rpcnotify(1, 'idx', {})", i));
    }

    /* Start sending the notifications. We use nvim.command() instead of passing
     * the commands via -c on the command line so that we block the test until the
     * notifications are actually pending.
     */
    timeout!(nvim.command(nvim_cmds.join("|").as_str()))
        .await
        .unwrap();

    /* Unblock notifications in the reverse order that they were sent, if
     * notifications are being handled sequentially then they should still be
     * processed in their original order.
     */
    for notifier in notifiers.iter().rev() {
        notifier.notify_one();
        /* Yield once, so that we guarantee the notification handler for this
         * notification executes pre-maturely if it was handled out of order.
         */
        yield_now().await;
    }

    // Check the results
    for i in 0..ITERS {
        assert_eq!(i as u8, timeout!(result_receiver.recv()).await.unwrap());
    }
}
