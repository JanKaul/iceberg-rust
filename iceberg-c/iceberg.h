#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

template<typename T = void>
struct Box;

struct CCatalog;

struct CObjectStore;

template<typename T = void>
struct Option;

extern "C" {

/// Constructor for rest catalog
Box<CCatalog> catalog_new_rest(const char *name,
                               const char *base_bath,
                               const char *access_token,
                               const CObjectStore *object_store);

/// Destructor for catalog
void catalog_free(Option<Box<CCatalog>> _object_store);

/// Load a table
Box<Relation> catalog_load_table(const CCatalog *catalog, const char *identifier);

/// Constructor for aws object_store
Box<CObjectStore> object_store_new_aws(const char *region,
                                       const char *bucket,
                                       const char *access_token);

/// Free object store memory
void object_store_free(Option<Box<CObjectStore>> _object_store);

/// Convert relation to table. Panics if conversion fails.
Box<Table> relation_to_table(Box<Relation> relation);

/// Destructor for relation
void relation_free(Option<Box<Relation>> _catalog);

/// Create new table transaction
Box<TableTransaction> table_new_transaction(Table *table);

/// Destructor for table
void table_free(Option<Box<Table>> _catalog);

/// Add new append operation to transaction
Box<TableTransaction> table_transaction_new_append(Box<TableTransaction> transaction,
                                                   const char *const *paths,
                                                   unsigned int num_paths);

/// Commit transaction freeing its memmory
void table_transaction_commit(Box<TableTransaction> transaction);

} // extern "C"
