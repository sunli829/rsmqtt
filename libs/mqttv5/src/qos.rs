use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};

/// Level of assurance for delivery of an Application Message.
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    IntoPrimitive,
    TryFromPrimitive,
    Serialize,
    Deserialize,
)]
#[repr(u8)]
pub enum Qos {
    /// At most once delivery
    AtMostOnce = 0,

    /// At least once delivery
    AtLeastOnce = 1,

    /// Exactly once delivery
    ExactlyOnce = 2,
}
