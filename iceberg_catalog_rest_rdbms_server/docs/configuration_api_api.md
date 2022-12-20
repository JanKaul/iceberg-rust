# configuration_api_api

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
**getConfig**](configuration_api_api.md#getConfig) | **GET** /v1/config | List all catalog configuration settings


# **getConfig**
> models::CatalogConfig getConfig(ctx, ctx, )
List all catalog configuration settings

 All REST clients should first call this route to get catalog configuration properties from the server to configure the catalog and its HTTP client. Configuration from the server consists of two sets of key/value pairs. - defaults -  properties that should be used as default configuration; applied before client configuration - overrides - properties that should be used to override client configuration; applied after defaults and client configuration  Catalog configuration is constructed by setting the defaults, then client- provided configuration, and finally overrides. The final property set is then used to configure the catalog.  For example, a default configuration property might set the size of the client pool, which can be replaced with a client-specific setting. An override might be used to set the warehouse location, which is stored on the server rather than in client configuration.  Common catalog configuration settings are documented at https://iceberg.apache.org/configuration/#catalog-properties 

### Required Parameters
This endpoint does not need any parameter.

### Return type

[**models::CatalogConfig**](CatalogConfig.md)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

