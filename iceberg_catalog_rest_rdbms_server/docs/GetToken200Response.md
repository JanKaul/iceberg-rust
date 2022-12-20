# GetToken200Response

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**access_token** | **String** | The access token, for client credentials or token exchange | 
**token_type** | **String** | Access token type for client credentials or token exchange  See https://datatracker.ietf.org/doc/html/rfc6749#section-7.1 | 
**expires_in** | **isize** | Lifetime of the access token in seconds for client credentials or token exchange | [optional] [default to None]
**issued_token_type** | [***models::TokenType**](TokenType.md) |  | [optional] [default to None]
**refresh_token** | **String** | Refresh token for client credentials or token exchange | [optional] [default to None]
**scope** | **String** | Authorization scope for client credentials or token exchange | [optional] [default to None]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


