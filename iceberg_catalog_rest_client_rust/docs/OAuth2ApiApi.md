# \OAuth2ApiApi

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_token**](OAuth2ApiApi.md#get_token) | **POST** /v1/oauth/tokens | Get a token using an OAuth2 flow



## get_token

> crate::models::GetToken200Response get_token(grant_type, scope, client_id, client_secret, requested_token_type, subject_token, subject_token_type, actor_token, actor_token_type)
Get a token using an OAuth2 flow

Exchange credentials for a token using the OAuth2 client credentials flow or token exchange.  This endpoint is used for three purposes - 1. To exchange client credentials (client ID and secret) for an access token This uses the client credentials flow. 2. To exchange a client token and an identity token for a more specific access token This uses the token exchange flow. 3. To exchange an access token for one with the same claims and a refreshed expiration period This uses the token exchange flow.  For example, a catalog client may be configured with client credentials from the OAuth2 Authorization flow. This client would exchange its client ID and secret for an access token using the client credentials request with this endpoint (1). Subsequent requests would then use that access token.  Some clients may also handle sessions that have additional user context. These clients would use the token exchange flow to exchange a user token (the \"subject\" token) from the session for a more specific access token for that user, using the catalog's access token as the \"actor\" token (2). The user ID token is the \"subject\" token and can be any token type allowed by the OAuth2 token exchange flow, including a unsecured JWT token with a sub claim. This request should use the catalog's bearer token in the \"Authorization\" header.  Clients may also use the token exchange flow to refresh a token that is about to expire by sending a token exchange request (3). The request's \"subject\" token should be the expiring token. This request should use the subject token in the \"Authorization\" header.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**grant_type** | Option<**String**> |  |  |
**scope** | Option<**String**> |  |  |
**client_id** | Option<**String**> | Client ID  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header. |  |
**client_secret** | Option<**String**> | Client secret  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header. |  |
**requested_token_type** | Option<[**crate::models::TokenType**](TokenType.md)> |  |  |
**subject_token** | Option<**String**> | Subject token for token exchange request |  |
**subject_token_type** | Option<[**crate::models::TokenType**](TokenType.md)> |  |  |
**actor_token** | Option<**String**> | Actor token for token exchange request |  |
**actor_token_type** | Option<[**crate::models::TokenType**](TokenType.md)> |  |  |

### Return type

[**crate::models::GetToken200Response**](getToken_200_response.md)

### Authorization

[BearerAuth](../README.md#BearerAuth), [OAuth2](../README.md#OAuth2)

### HTTP request headers

- **Content-Type**: application/x-www-form-urlencoded
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

