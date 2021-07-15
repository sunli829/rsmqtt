use bytestring::ByteString;
use tokio::sync::mpsc;

use crate::command::{Command, UnsubscribeCommand};
use crate::error::Error;
use crate::Result;

pub struct UnsubscribeBuilder {
    tx_command: mpsc::Sender<Command>,
    filters: Vec<ByteString>,
}

impl UnsubscribeBuilder {
    pub(crate) fn new(tx_command: mpsc::Sender<Command>) -> Self {
        Self {
            tx_command,
            filters: Vec::new(),
        }
    }

    pub fn filter(mut self, path: impl Into<ByteString>) -> Self {
        self.filters.push(path.into());
        self
    }

    pub async fn send(self) -> Result<()> {
        self.tx_command
            .send(Command::Unsubscribe(UnsubscribeCommand {
                filters: self.filters,
            }))
            .await
            .map_err(|_| Error::Closed)
    }
}
