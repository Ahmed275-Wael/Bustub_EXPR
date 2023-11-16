#include <memory>

#include "common/dberr.h"
#include "catalog/schema.h"
#include "storage/table/tuple.h"
#include "type/value.h"
namespace bustub{

class Transaction;

class index {
public:
  explicit index(index_id_t index_id, IndexSchema *key_schema)
          : index_id_(index_id), key_schema_(key_schema) {}
  explicit index(std::string index_name, IndexSchema *key_schema)
          : index_name_(index_name), key_schema_(key_schema) {}
  virtual ~index() {}

  virtual void InsertEntry(const Tuple &key, RID rid, Transaction *transaction) = 0;

  virtual void DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) = 0;

  virtual void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) = 0;

  virtual void Destroy() = 0;

protected:
  index_id_t index_id_;
  std::string index_name_;
  IndexSchema *key_schema_;
};
}