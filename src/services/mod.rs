pub mod chat;
pub mod discovery;

use anyhow::Result;

pub trait Services {
    fn run(&self) -> impl std::future::Future<Output = Result<()>> + Send;
}
