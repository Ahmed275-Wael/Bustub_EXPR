// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // seq_scan_executor.cpp
// //
// // Identification: src/execution/seq_scan_executor.cpp
// //
// // Copyright (c) 2015-2021, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include "execution/executors/seq_scan_executor.h"

// namespace bustub {

// SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan), tableIterator(nullptr, RID(), nullptr) {}

// void SeqScanExecutor::Init() {
//     table_oid_t tableId =  this->plan_->GetTableOid();  
//    ExecutorContext *exec_ctx = this->GetExecutorContext();
//    Catalog *catalog = exec_ctx->GetCatalog();
//    TableInfo*tableInfo = catalog->GetTable(tableId);
    
//    TableIterator iterator = tableInfo->table_->Begin(exec_ctx->GetTransaction());
//    this->tableIterator = iterator;
//  }

// auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    
//     Catalog *catalog = this->GetExecutorContext()->GetCatalog();
//     TableInfo*tableInfo = catalog->GetTable(this->plan_->GetTableOid());
//     if (tableIterator == tableInfo->table_->End()) {
//         return false;
//     }
//      *tuple = *tableIterator;
//      *rid = tuple->GetRid();
//       tableIterator++;
//       return true;  
    
//     }

// }  // namespace bustub
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iter_({nullptr, RID(), nullptr}) {}

void SeqScanExecutor::Init() {
  iter_ = GetExecutorContext()
              ->GetCatalog()
              ->GetTable(plan_->GetTableOid())
              ->table_->Begin(GetExecutorContext()->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End()) {
    return false;
  }

  *tuple = *iter_;
  *rid = iter_->GetRid();
  iter_++;
  return true;
}

}  // namespace bustub