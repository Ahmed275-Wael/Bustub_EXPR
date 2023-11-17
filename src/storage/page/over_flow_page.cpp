//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// table_page.cpp
//
// Identification: src/storage/page/table_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/over_flow_page.h"

#include <cassert>

namespace bustub {

void OverFlowPage::Init(page_id_t page_id, uint32_t page_size, page_id_t nextPageId, LogManager *log_manager,
                     Transaction *txn) {
//   LOG_DEBUG("OverflowPage id initalization %d", page_id);
  memcpy(GetData(), &page_id, sizeof(page_id));
  SetNextPageId(nextPageId);
  SetTupleSize(0);
}

auto OverFlowPage::InsertOverFlowedData(char*data,int size) -> bool {
        memcpy(GetData() + SIZE_OVER_FLOW_HEADER, data, size);  
        SetTupleSize(size);    
 
       return true;                   
}

auto OverFlowPage::MarkDelete(const RID &rid, Transaction *txn, LockManager *lock_manager, LogManager *log_manager)
    -> bool {
 return true;
}

auto OverFlowPage::UpdateTuple(const Tuple &new_tuple, Tuple *old_tuple, const RID &rid, Transaction *txn,
                            LockManager *lock_manager, LogManager *log_manager) -> bool {
    return true;
}

void OverFlowPage::ApplyDelete(const RID &rid, Transaction *txn, LogManager *log_manager) {
 
}

void OverFlowPage::RollbackDelete(const RID &rid, Transaction *txn, LogManager *log_manager) {
 
 
}

auto OverFlowPage::GetTuple(const RID &rid, Tuple *tuple, Transaction *txn, LockManager *lock_manager) -> bool {
    return true;
}

auto OverFlowPage::GetTupleRid(RID *first_rid) -> bool {
    return true;
}

 
}  // namespace bustub
