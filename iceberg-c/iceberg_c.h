typedef void* Relation;
typedef void* Table;
typedef void* TableTransaction;
typedef void* TableBuilder;


#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct ArcCatalog ArcCatalog;

typedef struct ArcObjectStore ArcObjectStore;

/**
 * Constructor for rest catalog
 */
struct ArcCatalog *catalog_new_rest(const char *name,
                                    const char *base_bath,
                                    const char *access_token,
                                    const struct ArcObjectStore *object_store);

/**
 * Destructor for catalog
 */
void catalog_free(struct ArcCatalog *_object_store);

/**
 * Load a table
 */
Relation *catalog_load_table(const struct ArcCatalog *catalog, const char *identifier);

/**
 * Check if table exists
 */
bool catalog_table_exists(const struct ArcCatalog *catalog, const char *identifier);

/**
 * Constructor for aws object_store with an access token
 */
struct ArcObjectStore *object_store_new_aws_token(const char *region,
                                                  const char *bucket,
                                                  const char *access_token);

/**
 * Constructor for aws object_store with an access_key_id and secret_access_key
 */
struct ArcObjectStore *object_store_new_aws_access_key(const char *region,
                                                       const char *bucket,
                                                       const char *access_key_id,
                                                       const char *secret_access_key);

/**
 * Free object store memory
 */
void object_store_free(struct ArcObjectStore *_object_store);

/**
 * Convert relation to table. Panics if conversion fails.
 */
Table *relation_to_table(Relation *relation);

/**
 * Destructor for relation
 */
void relation_free(Relation *_catalog);

/**
 * Create new table transaction
 */
TableTransaction *table_new_transaction(Table *table);

/**
 * Destructor for table
 */
void table_free(Table *_catalog);

/**
 * Create new metastore table
 */
TableBuilder *table_builder_new_metastore(const char *base_path,
                                          const char *schema,
                                          const char *identifier,
                                          const struct ArcCatalog *catalog);

/**
 * Commit table builder and create table
 */
Table *table_builder_commit(TableBuilder *table_builder);

/**
 * Add new append operation to transaction
 */
TableTransaction *table_transaction_new_append(TableTransaction *transaction,
                                               const char *const *paths,
                                               unsigned int num_paths);

/**
 * Commit transaction freeing its memmory
 */
void table_transaction_commit(TableTransaction *transaction);
