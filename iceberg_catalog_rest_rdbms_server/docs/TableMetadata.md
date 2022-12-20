# TableMetadata

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**format_version** | **u8** |  | 
**table_uuid** | **String** |  | 
**location** | **String** |  | [optional] [default to None]
**last_updated_ms** | **isize** |  | [optional] [default to None]
**properties** | **std::collections::HashMap<String, String>** |  | [optional] [default to None]
**schemas** | [**Vec<models::Schema>**](Schema.md) |  | [optional] [default to None]
**current_schema_id** | **isize** |  | [optional] [default to None]
**last_column_id** | **isize** |  | [optional] [default to None]
**partition_specs** | [**Vec<models::PartitionSpec>**](PartitionSpec.md) |  | [optional] [default to None]
**default_spec_id** | **isize** |  | [optional] [default to None]
**last_partition_id** | **isize** |  | [optional] [default to None]
**sort_orders** | [**Vec<models::SortOrder>**](SortOrder.md) |  | [optional] [default to None]
**default_sort_order_id** | **isize** |  | [optional] [default to None]
**snapshots** | [**Vec<models::Snapshot>**](Snapshot.md) |  | [optional] [default to None]
**refs** | [**std::collections::HashMap<String, models::SnapshotReference>**](SnapshotReference.md) |  | [optional] [default to None]
**current_snapshot_id** | **isize** |  | [optional] [default to None]
**snapshot_log** | [**Vec<models::SnapshotLogInner>**](SnapshotLog_inner.md) |  | [optional] [default to None]
**metadata_log** | [**Vec<models::MetadataLogInner>**](MetadataLog_inner.md) |  | [optional] [default to None]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


