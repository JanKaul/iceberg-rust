# CreateTableRequest

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **String** |  | 
**location** | **String** |  | [optional] [default to None]
**schema** | [***models::Schema**](Schema.md) |  | 
**partition_spec** | [***models::PartitionSpec**](PartitionSpec.md) |  | [optional] [default to None]
**write_order** | [***models::SortOrder**](SortOrder.md) |  | [optional] [default to None]
**stage_create** | **bool** |  | [optional] [default to None]
**properties** | **std::collections::HashMap<String, String>** |  | [optional] [default to None]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


