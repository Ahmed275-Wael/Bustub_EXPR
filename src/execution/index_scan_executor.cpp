//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() { 

ExecutorContext *exec_ctx = this->GetExecutorContext();
Catalog *catalog = exec_ctx->GetCatalog();
IndexInfo* indexInfo = catalog->GetIndex(plan_->GetIndexOid());
auto *tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(indexInfo->index_.get());
recordsIds.clear();
for (BPlusTreeIndexIteratorForOneIntegerColumn iterator = tree_->GetBeginIterator(); !iterator.IsEnd();  ++iterator) {
        recordsIds.push_back((*iterator).second);
}
index = 0;

 }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
 
    if ((size_t)index >= recordsIds.size()) return false;
    ExecutorContext *exec_ctx = this->GetExecutorContext();
    Catalog *catalog = exec_ctx->GetCatalog();
    IndexInfo* indexInfo = catalog->GetIndex(plan_->GetIndexOid());
    TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(indexInfo->table_name_);
    table_info->table_->GetTuple(recordsIds[index], tuple, exec_ctx->GetTransaction());
    index++;
    return true;
}

}  // namespace bustub
 
// #include "execution/executors/index_scan_executor.h"

// namespace bustub {
// IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
//     : AbstractExecutor(exec_ctx), plan_(plan) {}

// void IndexScanExecutor::Init() {
//   IndexInfo *index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());
//   auto *tree = reinterpret_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
//   rids_.clear();
//   for (BPlusTreeIndexIteratorForOneIntegerColumn iter = tree->GetBeginIterator(); !iter.IsEnd(); ++iter) {
//     rids_.push_back((*iter).second);
//   }
//   iter_ = rids_.begin();
// }

// auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   if (iter_ == rids_.end()) {
//     return false;
//   }

//   IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
//   TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
//   Transaction *tx = GetExecutorContext()->GetTransaction();

//   *rid = *iter_;
//   table_info->table_->GetTuple(*rid, tuple, tx);
//   ++iter_;

//   return true;
// }

// }  // namespace bustub