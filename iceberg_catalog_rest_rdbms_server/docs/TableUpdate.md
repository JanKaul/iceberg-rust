# TableUpdate

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **String** |  | 
**format_version** | **isize** |  | 
**schema** | [***models::Schema**](Schema.md) |  | 
**schema_id** | **isize** | Schema ID to set as current, or -1 to set last added schema | 
**spec** | [***models::PartitionSpec**](PartitionSpec.md) |  | 
**spec_id** | **isize** | Partition spec ID to set as the default, or -1 to set last added spec | 
**sort_order** | [***models::SortOrder**](SortOrder.md) |  | 
**sort_order_id** | **isize** | Sort order ID to set as the default, or -1 to set last added sort order | 
**snapshot** | [***models::Snapshot**](Snapshot.md) |  | 
**r#type** | **String** |  | 
**snapshot_id** | **i64** |  | 
**max_ref_age_ms** | **i64** |  | [optional] [default to None]
**max_snapshot_age_ms** | **i64** |  | [optional] [default to None]
**min_snapshots_to_keep** | **isize** |  | [optional] [default to None]
**ref_name** | **String** |  | 
**snapshot_ids** | **Vec<i64>** |  | 
**location** | **String** |  | 
**updates** | **std::collections::HashMap<String, String>** |  | 
**removals** | **Vec<String>** |  | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


