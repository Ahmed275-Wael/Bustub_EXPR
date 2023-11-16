
#include <memory>

#include "catalog/table.h"
#include "storage/index/generic_key.h"
#include "storage/index/b_plus_tree_index.h"
#include "catalog/schema.h"

namespace bustub{
class Index_Metadata {
  friend class Index_Info;

public:
  static Index_Metadata *Create(const index_id_t index_id, const std::string &index_name,
                               const table_id_t table_id, const std::vector<uint32_t> &key_map,
                               MemHeap *heap);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, Index_Metadata *&index_meta, MemHeap *heap);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; }

private:
  Index_Metadata() = delete;

  explicit Index_Metadata(const index_id_t index_id, const std::string &index_name,
                         const table_id_t table_id, const std::vector<uint32_t> &key_map) {
    this->index_id_ = index_id;
    this->table_id_ = table_id;
    this->index_name_ = index_name;
    for (auto i : key_map) {
      this->key_map_.push_back(i);
    }
  }

private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  std::string index_name_;
  table_id_t table_id_;
  std::vector<uint32_t> key_map_;  /** The mapping of index key to tuple key */
};

/**
 * The Index_Info class maintains metadata about a index.
 */
class Index_Info {
public:
  static Index_Info *Create(MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(Index_Info));
    return new(buf)Index_Info();
  }

  ~Index_Info() {
    delete heap_;
  }

  void Init(Index_Metadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager) {
    // Step1: init index metadata and table info
    // Step2: mapping index key to key schema
    // Step3: call CreateIndex to create the index
    // TAKE CARE: do not actually know the mechanism how this function works
    meta_data_ = meta_data;
    table_info_ = table_info;
    key_schema_ = Schema::CopySchema(table_info->GetSchema(), meta_data->GetKeyMapping(), heap_);
    index_ = CreateIndex(buffer_pool_manager);
  }

  inline Index *GetIndex() {
    return index_;
  }

  inline std::string GetIndexName() { return meta_data_->GetIndexName(); }

  inline IndexSchema *GetIndexKeySchema() { return key_schema_; }

  inline MemHeap *GetMemHeap() const { return heap_; }

  inline TableInfo *GetTableInfo() const { return table_info_; }

  inline Index_Metadata *GetMetaData() const { return this->meta_data_; }

private:
  explicit Index_Info() : meta_data_{nullptr}, index_{nullptr}, table_info_{nullptr},
                         key_schema_{nullptr}, heap_(new SimpleMemHeap()) {}

  Index *CreateIndex(BufferPoolManager *buffer_pool_manager) {
    void *buf = heap_->Allocate(sizeof(BPlusTreeIndex<GenericKey<64>, RID, GenericComparator<64>>));
    auto meta = std::make_unique<IndexMetadata>(GetIndexName(), GetTableInfo()->GetTableName(), key_schema_, meta_data_->GetKeyMapping());
    return new (buf) BPlusTreeIndex<GenericKey<4>, RID, GenericComparator<4>>(std::move(meta),
                                                                                  buffer_pool_manager);
  }

private:
  Index_Metadata *meta_data_;
  //std::unique_ptr<IndexMetadata> metadata;
  Index *index_;
  TableInfo *table_info_;
  IndexSchema *key_schema_;
  MemHeap *heap_;
};
}

