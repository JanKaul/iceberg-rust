#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

template<typename T = void>
struct Box;

struct CCatalog;

struct CObjectStore;

struct CRelation;

struct CTable;

struct CTableTransaction;

extern "C" {

Box<CCatalog> catalog_new_rest(const char *name,
                               const char *base_bath,
                               const char *access_token,
                               const CObjectStore *object_store);

Box<CRelation> catalog_load_table(const CCatalog *catalog, const char *identifier);

Box<CObjectStore> object_store_new_aws(const char *region,
                                       const char *bucket,
                                       const char *access_token);

void object_store_free(Box<CObjectStore> _object_store);

Box<CTable> relation_to_table(Box<CRelation> relation);

Box<CTableTransaction> table_new_transaction(CTable *table);

Box<CTableTransaction> table_transaction_new_append(Box<CTableTransaction> transaction,
                                                    const char *const *paths,
                                                    unsigned int num_paths);

void table_transaction_commit(Box<CTableTransaction> transaction);

} // extern "C"
