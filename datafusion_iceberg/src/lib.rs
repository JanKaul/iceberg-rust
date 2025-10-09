pub mod catalog;
pub mod error;
pub mod materialized_view;
pub mod optimizer;
pub mod planner;
mod pruning_statistics;
mod statistics;
pub mod table;

pub use crate::table::DataFusionTable;
