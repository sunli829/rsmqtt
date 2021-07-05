use codec::Packet;
use serde::Deserialize;

use bytestring::ByteString;
use service::ServiceConfig;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Step {
    Connect,
    Disconnect,
    Send {
        packet: Packet,
    },
    #[serde(rename = "recv")]
    Receive {
        packet: Packet,
        after: Option<u64>,
    },
    Eof,
    Delay {
        duration: u64,
    },
    Parallel {
        steps: Vec<Step>,
    },
    Sequence {
        id: Option<ByteString>,
        client_id: Option<ByteString>,
        steps: Vec<Step>,
    },
}

#[derive(Debug, Deserialize)]
pub struct Suite {
    #[serde(default)]
    pub config: ServiceConfig,
    pub step: Step,
    #[serde(default)]
    pub disable: bool,
}