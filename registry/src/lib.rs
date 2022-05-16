pub mod discovery;
pub mod protos;
pub use anyhow::{self};
use std::time::Duration;
pub mod client;
pub mod utils;
pub use log;
pub use nats;
pub use tokio;

pub type ResultType<F, E = anyhow::Error> = anyhow::Result<F, E>;

pub static DEFAULT_PUBLISH_PRIFEX: &str = "node.publish";
pub static DEFAULT_DISCOVERY_PRIFEX: &str = "node.discovery";
pub static DEFAULT_NODE_EVENT_PRIFEX: &str = "node.event";
pub static DEFAULT_NODE_BROADCAST_PRIFEX: &str = "node.broadcast";

lazy_static::lazy_static! {
    pub static ref DEFAULT_LIVE_CYCLE: Duration = Duration::from_secs(2);
    pub static ref DEFAULT_EXPIRE: u64 = 5;
}
