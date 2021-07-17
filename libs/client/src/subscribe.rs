use anyhow::Result;
use bytestring::ByteString;
use codec::{Qos, RetainHandling, SubscribeFilter};
use tokio::sync::mpsc;

use crate::command::{Command, SubscribeCommand};

pub struct SubscribeBuilder {
    tx_command: mpsc::Sender<Command>,
    filters: Vec<SubscribeFilter>,
}

impl SubscribeBuilder {
    pub(crate) fn new(tx_command: mpsc::Sender<Command>) -> Self {
        Self {
            tx_command,
            filters: Vec::new(),
        }
    }

    #[inline]
    pub fn filter(mut self, filter: FilterBuilder) -> Self {
        self.filters.push(SubscribeFilter {
            path: filter.path,
            qos: filter.qos,
            no_local: filter.no_local,
            retain_as_published: filter.retain_as_published,
            retain_handling: filter.retain_handling,
        });
        self
    }

    pub async fn send(self) -> Result<()> {
        self.tx_command
            .send(Command::Subscribe(SubscribeCommand {
                filters: self.filters,
            }))
            .await
            .map_err(|_| Error::Closed)
    }
}

pub struct FilterBuilder {
    path: ByteString,
    qos: Qos,
    no_local: bool,
    retain_as_published: bool,
    retain_handling: RetainHandling,
}

impl FilterBuilder {
    pub fn new(path: impl Into<ByteString>) -> Self {
        Self {
            path: path.into(),
            qos: Qos::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::OnEverySubscribe,
        }
    }

    #[inline]
    pub fn qos(self, qos: Qos) -> Self {
        Self { qos, ..self }
    }

    #[inline]
    pub fn no_local(self) -> Self {
        Self {
            no_local: true,
            ..self
        }
    }

    #[inline]
    pub fn retain_as_published(self) -> Self {
        Self {
            retain_as_published: true,
            ..self
        }
    }

    #[inline]
    pub fn retain_handling(self, retain_handling: RetainHandling) -> Self {
        Self {
            retain_handling,
            ..self
        }
    }
}
