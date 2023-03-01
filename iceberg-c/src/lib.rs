use std::future::Future;

pub mod catalog;
pub mod object_store;
pub mod realtion;
pub mod table;

pub fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Runtime::new().unwrap().block_on(future)
}
