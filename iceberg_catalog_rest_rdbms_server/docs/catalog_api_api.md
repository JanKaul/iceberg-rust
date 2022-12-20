# catalog_api_api

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
**createNamespace**](catalog_api_api.md#createNamespace) | **POST** /v1/{prefix}/namespaces | Create a namespace
**createTable**](catalog_api_api.md#createTable) | **POST** /v1/{prefix}/namespaces/{namespace}/tables | Create a table in the given namespace
**dropNamespace**](catalog_api_api.md#dropNamespace) | **DELETE** /v1/{prefix}/namespaces/{namespace} | Drop a namespace from the catalog. Namespace must be empty.
**dropTable**](catalog_api_api.md#dropTable) | **DELETE** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Drop a table from the catalog
**listNamespaces**](catalog_api_api.md#listNamespaces) | **GET** /v1/{prefix}/namespaces | List namespaces, optionally providing a parent namespace to list underneath
**listTables**](catalog_api_api.md#listTables) | **GET** /v1/{prefix}/namespaces/{namespace}/tables | List all table identifiers underneath a given namespace
**loadNamespaceMetadata**](catalog_api_api.md#loadNamespaceMetadata) | **GET** /v1/{prefix}/namespaces/{namespace} | Load the metadata properties for a namespace
**loadTable**](catalog_api_api.md#loadTable) | **GET** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Load a table from the catalog
**renameTable**](catalog_api_api.md#renameTable) | **POST** /v1/{prefix}/tables/rename | Rename a table from its current name to a new name
**reportMetrics**](catalog_api_api.md#reportMetrics) | **POST** /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics | Send a metrics report to this endpoint to be processed by the backend
**tableExists**](catalog_api_api.md#tableExists) | **HEAD** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Check if a table exists
**updateProperties**](catalog_api_api.md#updateProperties) | **POST** /v1/{prefix}/namespaces/{namespace}/properties | Set or remove properties on a namespace
**updateTable**](catalog_api_api.md#updateTable) | **POST** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Commit updates to a table


# **createNamespace**
> models::CreateNamespace200Response createNamespace(ctx, ctx, prefix, optional)
Create a namespace

Create a namespace, with an optional set of properties. The server might also add properties, such as `last_modified_time` etc.

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
  **prefix** | **String**| An optional prefix in the path | 
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **String**| An optional prefix in the path | 
 **create_namespace_request** | [**CreateNamespaceRequest**](CreateNamespaceRequest.md)|  | 

### Return type

[**models::CreateNamespace200Response**](createNamespace_200_response.md)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **createTable**
> models::LoadTableResult createTable(ctx, ctx, prefix, namespace, optional)
Create a table in the given namespace

Create a table or start a create transaction, like atomic CTAS.  If `stage-create` is false, the table is created immediately.  If `stage-create` is true, the table is not created, but table metadata is initialized and returned. The service should prepare as needed for a commit to the table commit endpoint to complete the create transaction. The client uses the returned metadata to begin a transaction. To commit the transaction, the client sends all create and subsequent changes to the table commit route. Changes from the table create operation include changes like AddSchemaUpdate and SetCurrentSchemaUpdate that set the initial table state.

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
  **prefix** | **String**| An optional prefix in the path | 
  **namespace** | **String**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | 
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **String**| An optional prefix in the path | 
 **namespace** | **String**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | 
 **create_table_request** | [**CreateTableRequest**](CreateTableRequest.md)|  | 

### Return type

[**models::LoadTableResult**](LoadTableResult.md)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **dropNamespace**
> dropNamespace(ctx, ctx, prefix, namespace)
Drop a namespace from the catalog. Namespace must be empty.

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
  **prefix** | **String**| An optional prefix in the path | 
  **namespace** | **String**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | 

### Return type

 (empty response body)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **dropTable**
> dropTable(ctx, ctx, prefix, namespace, table, optional)
Drop a table from the catalog

Remove a table from the catalog

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
  **prefix** | **String**| An optional prefix in the path | 
  **namespace** | **String**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | 
  **table** | **String**| A table name | 
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **String**| An optional prefix in the path | 
 **namespace** | **String**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | 
 **table** | **String**| A table name | 
 **purge_requested** | **bool**| Whether the user requested to purge the underlying table's data and metadata | [default to false]

### Return type

 (empty response body)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **listNamespaces**
> models::ListNamespaces200Response listNamespaces(ctx, ctx, prefix, optional)
List namespaces, optionally providing a parent namespace to list underneath

List all namespaces at a certain level, optionally starting from a given parent namespace. For example, if table accounting.tax.paid exists, using 'SELECT NAMESPACE IN accounting' would translate into `GET /namespaces?parent=accounting` and must return a namespace, [\"accounting\", \"tax\"]. If `parent` is not provided, all top-level namespaces should be listed.

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
  **prefix** | **String**| An optional prefix in the path | 
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **String**| An optional prefix in the path | 
 **parent** | **String**| An optional namespace, underneath which to list namespaces. If not provided or empty, all top-level namespaces should be listed. If parent is a multipart namespace, the parts must be separated by the unit separator (`0x1F`) byte. | 

### Return type

[**models::ListNamespaces200Response**](listNamespaces_200_response.md)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **listTables**
> models::ListTables200Response listTables(ctx, ctx, prefix, namespace)
List all table identifiers underneath a given namespace

Return all table identifiers under this namespace

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
  **prefix** | **String**| An optional prefix in the path | 
  **namespace** | **String**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | 

### Return type

[**models::ListTables200Response**](listTables_200_response.md)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **loadNamespaceMetadata**
> models::LoadNamespaceMetadata200Response loadNamespaceMetadata(ctx, ctx, prefix, namespace)
Load the metadata properties for a namespace

Return all stored metadata properties for a given namespace

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
  **prefix** | **String**| An optional prefix in the path | 
  **namespace** | **String**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | 

### Return type

[**models::LoadNamespaceMetadata200Response**](loadNamespaceMetadata_200_response.md)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **loadTable**
> models::LoadTableResult loadTable(ctx, ctx, prefix, namespace, table)
Load a table from the catalog

Load a table from the catalog.  The response contains both configuration and table metadata. The configuration, if non-empty is used as additional configuration for the table that overrides catalog configuration. For example, this configuration may change the FileIO implemented used for the table.  The response also contains the table's full metadata.  The catalog configuration may contain credentials that should be used for subsequent requests for the table. The configuration key \"token\" is used to pass an access token to be used as a bearer token for table requests. Otherwise, a token may be passed using a RFC 8693 token type as a configuration key. For example, \"urn:ietf:params:oauth:token-type:jwt=<JWT-token>\".

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
  **prefix** | **String**| An optional prefix in the path | 
  **namespace** | **String**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | 
  **table** | **String**| A table name | 

### Return type

[**models::LoadTableResult**](LoadTableResult.md)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **renameTable**
> renameTable(ctx, ctx, prefix, rename_table_request)
Rename a table from its current name to a new name

Rename a table from one identifier to another. It's valid to move a table across namespaces, but the server implementation is not required to support it.

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
  **prefix** | **String**| An optional prefix in the path | 
  **rename_table_request** | [**RenameTableRequest**](RenameTableRequest.md)| Current table identifier to rename and new table identifier to rename to | 

### Return type

 (empty response body)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **reportMetrics**
> reportMetrics(ctx, ctx, prefix, namespace, table, report_metrics_request)
Send a metrics report to this endpoint to be processed by the backend

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
  **prefix** | **String**| An optional prefix in the path | 
  **namespace** | **String**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | 
  **table** | **String**| A table name | 
  **report_metrics_request** | [**ReportMetricsRequest**](ReportMetricsRequest.md)| The request containing the metrics report to be sent | 

### Return type

 (empty response body)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **tableExists**
> tableExists(ctx, ctx, prefix, namespace, table)
Check if a table exists

Check if a table exists within a given namespace. This request does not return a response body.

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
  **prefix** | **String**| An optional prefix in the path | 
  **namespace** | **String**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | 
  **table** | **String**| A table name | 

### Return type

 (empty response body)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **updateProperties**
> models::UpdateProperties200Response updateProperties(ctx, ctx, prefix, namespace, optional)
Set or remove properties on a namespace

Set and/or remove properties on a namespace. The request body specifies a list of properties to remove and a map of key value pairs to update. Properties that are not in the request are not modified or removed by this call. Server implementations are not required to support namespace properties.

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
  **prefix** | **String**| An optional prefix in the path | 
  **namespace** | **String**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | 
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **String**| An optional prefix in the path | 
 **namespace** | **String**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | 
 **update_namespace_properties_request** | [**UpdateNamespacePropertiesRequest**](UpdateNamespacePropertiesRequest.md)|  | 

### Return type

[**models::UpdateProperties200Response**](updateProperties_200_response.md)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **updateTable**
> models::UpdateTable200Response updateTable(ctx, ctx, prefix, namespace, table, optional)
Commit updates to a table

Commit updates to a table.  Commits have two parts, requirements and updates. Requirements are assertions that will be validated before attempting to make and commit changes. For example, `assert-ref-snapshot-id` will check that a named ref's snapshot ID has a certain value.  Updates are changes to make to table metadata. For example, after asserting that the current main ref is at the expected snapshot, a commit may add a new child snapshot and set the ref to the new snapshot id.  Create table transactions that are started by createTable with `stage-create` set to true are committed using this route. Transactions should include all changes to the table, including table initialization, like AddSchemaUpdate and SetCurrentSchemaUpdate. The `assert-create` requirement is used to ensure that the table was not created concurrently.

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
 **ctx** | **context.Context** | context containing the authentication | nil if no authentication
  **prefix** | **String**| An optional prefix in the path | 
  **namespace** | **String**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | 
  **table** | **String**| A table name | 
 **optional** | **map[string]interface{}** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a map[string]interface{}.

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **String**| An optional prefix in the path | 
 **namespace** | **String**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | 
 **table** | **String**| A table name | 
 **commit_table_request** | [**CommitTableRequest**](CommitTableRequest.md)|  | 

### Return type

[**models::UpdateTable200Response**](updateTable_200_response.md)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

