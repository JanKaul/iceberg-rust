#![allow(unused_imports)]
#![allow(clippy::too_many_arguments)]

#[macro_use]
extern crate serde_derive;
extern crate reqwest;
extern crate serde;
extern crate serde_json;
extern crate serde_repr;
extern crate url;

#[allow(clippy::all)]
pub mod apis;
pub mod catalog;
pub mod configuration_rewriter;
pub mod error;
#[allow(clippy::all)]
pub mod models;
pub mod oauth2_client_credentials;
