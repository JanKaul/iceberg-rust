#![allow(missing_docs, unused_variables, trivial_casts)]


#[allow(unused_imports)]
use futures::{future, Stream, stream};
#[allow(unused_imports)]
use openapi_client::{Api, ApiNoContext, Client, ContextWrapperExt, models,
                      CreateNamespaceResponse,
                      CreateTableResponse,
                      DropNamespaceResponse,
                      DropTableResponse,
                      ListNamespacesResponse,
                      ListTablesResponse,
                      LoadNamespaceMetadataResponse,
                      LoadTableResponse,
                      RenameTableResponse,
                      ReportMetricsResponse,
                      TableExistsResponse,
                      UpdatePropertiesResponse,
                      UpdateTableResponse,
                      GetConfigResponse,
                      GetTokenResponse,
                     };
use clap::{App, Arg};

#[allow(unused_imports)]
use log::info;

// swagger::Has may be unused if there are no examples
#[allow(unused_imports)]
use swagger::{AuthData, ContextBuilder, EmptyContext, Has, Push, XSpanIdString};

type ClientContext = swagger::make_context_ty!(ContextBuilder, EmptyContext, Option<AuthData>, XSpanIdString);

// rt may be unused if there are no examples
#[allow(unused_mut)]
fn main() {
    env_logger::init();

    let matches = App::new("client")
        .arg(Arg::with_name("operation")
            .help("Sets the operation to run")
            .possible_values(&[
                "CreateNamespace",
                "CreateTable",
                "DropNamespace",
                "DropTable",
                "ListNamespaces",
                "ListTables",
                "LoadNamespaceMetadata",
                "LoadTable",
                "TableExists",
                "UpdateProperties",
                "UpdateTable",
                "GetConfig",
                "GetToken",
            ])
            .required(true)
            .index(1))
        .arg(Arg::with_name("https")
            .long("https")
            .help("Whether to use HTTPS or not"))
        .arg(Arg::with_name("host")
            .long("host")
            .takes_value(true)
            .default_value("localhost")
            .help("Hostname to contact"))
        .arg(Arg::with_name("port")
            .long("port")
            .takes_value(true)
            .default_value("8080")
            .help("Port to contact"))
        .get_matches();

    let is_https = matches.is_present("https");
    let base_url = format!("{}://{}:{}",
                           if is_https { "https" } else { "http" },
                           matches.value_of("host").unwrap(),
                           matches.value_of("port").unwrap());

    let context: ClientContext =
        swagger::make_context!(ContextBuilder, EmptyContext, None as Option<AuthData>, XSpanIdString::default());

    let mut client : Box<dyn ApiNoContext<ClientContext>> = if matches.is_present("https") {
        // Using Simple HTTPS
        let client = Box::new(Client::try_new_https(&base_url)
            .expect("Failed to create HTTPS client"));
        Box::new(client.with_context(context))
    } else {
        // Using HTTP
        let client = Box::new(Client::try_new_http(
            &base_url)
            .expect("Failed to create HTTP client"));
        Box::new(client.with_context(context))
    };

    let mut rt = tokio::runtime::Runtime::new().unwrap();

    match matches.value_of("operation") {
        Some("CreateNamespace") => {
            let result = rt.block_on(client.create_namespace(
                  "prefix_example".to_string(),
                  None
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        Some("CreateTable") => {
            let result = rt.block_on(client.create_table(
                  "prefix_example".to_string(),
                  "accounting".to_string(),
                  None
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        Some("DropNamespace") => {
            let result = rt.block_on(client.drop_namespace(
                  "prefix_example".to_string(),
                  "accounting".to_string()
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        Some("DropTable") => {
            let result = rt.block_on(client.drop_table(
                  "prefix_example".to_string(),
                  "accounting".to_string(),
                  "sales".to_string(),
                  Some(true)
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        Some("ListNamespaces") => {
            let result = rt.block_on(client.list_namespaces(
                  "prefix_example".to_string(),
                  Some("accounting%1Ftax".to_string())
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        Some("ListTables") => {
            let result = rt.block_on(client.list_tables(
                  "prefix_example".to_string(),
                  "accounting".to_string()
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        Some("LoadNamespaceMetadata") => {
            let result = rt.block_on(client.load_namespace_metadata(
                  "prefix_example".to_string(),
                  "accounting".to_string()
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        Some("LoadTable") => {
            let result = rt.block_on(client.load_table(
                  "prefix_example".to_string(),
                  "accounting".to_string(),
                  "sales".to_string()
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        /* Disabled because there's no example.
        Some("RenameTable") => {
            let result = rt.block_on(client.rename_table(
                  "prefix_example".to_string(),
                  ???
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        */
        /* Disabled because there's no example.
        Some("ReportMetrics") => {
            let result = rt.block_on(client.report_metrics(
                  "prefix_example".to_string(),
                  "accounting".to_string(),
                  "sales".to_string(),
                  ???
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        */
        Some("TableExists") => {
            let result = rt.block_on(client.table_exists(
                  "prefix_example".to_string(),
                  "accounting".to_string(),
                  "sales".to_string()
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        Some("UpdateProperties") => {
            let result = rt.block_on(client.update_properties(
                  "prefix_example".to_string(),
                  "accounting".to_string(),
                  None
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        Some("UpdateTable") => {
            let result = rt.block_on(client.update_table(
                  "prefix_example".to_string(),
                  "accounting".to_string(),
                  "sales".to_string(),
                  None
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        Some("GetConfig") => {
            let result = rt.block_on(client.get_config(
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        Some("GetToken") => {
            let result = rt.block_on(client.get_token(
                  Some("grant_type_example".to_string()),
                  Some("scope_example".to_string()),
                  Some("client_id_example".to_string()),
                  Some("client_secret_example".to_string()),
                  None,
                  Some("subject_token_example".to_string()),
                  None,
                  Some("actor_token_example".to_string()),
                  None
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        _ => {
            panic!("Invalid operation provided")
        }
    }
}
