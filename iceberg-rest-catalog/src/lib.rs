#![allow(unused_imports)]

#[macro_use]
extern crate serde_derive;

extern crate reqwest;
extern crate serde;
extern crate serde_json;
extern crate url;

#[allow(clippy::all)]
pub mod apis;
pub mod catalog;
pub mod error;
#[allow(clippy::all)]
pub mod models;
