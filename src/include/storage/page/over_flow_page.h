//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// table_page.h
//
// Identification: src/include/storage/page/table_page.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstring>

#include "common/rid.h"
#include "concurrency/lock_manager.h"
#include "recovery/log_manager.h"
#include "storage/page/page.h"
#include "storage/table/tuple.h"

 
namespace bustub {

/**
 * 
 * 
 *  
 *  Header format (size in bytes):
 *  -----------------------------------------
 *  | PageId (4)| NextPageId (4)|Tuple Size(4)|
 *  -------------------------------------------
 *
 */
class OverFlowPage : public Page {
 public:
  /**
   * Initialize the TablePage header.
   * @param page_id the page ID of this table page
   * @param page_size the size of this table page
   * @param prev_page_id the previous table page ID
   * @param log_manager the log manager in use
   * @param txn the transaction that this page is created in
   */
void Init(page_id_t page_id, uint32_t page_size, page_id_t nextPageId, LogManager *log_manager,
                     Transaction *txn);

  /** @return the page ID of this table page */
  auto GetOverFlowPageId() -> page_id_t { return *reinterpret_cast<page_id_t *>(GetData()); }

 
  /** @return the page ID of the next table page */
  auto GetNextPageId() -> page_id_t { return *reinterpret_cast<page_id_t *>(GetData() + OFFSET_NEXT_PAGE_ID); }

  

  /** Set the page id of the next page in the table. */
  void SetNextPageId(page_id_t next_page_id) {
    memcpy(GetData() + OFFSET_NEXT_PAGE_ID, &next_page_id, sizeof(page_id_t));
  }
  void SetTupleSize(uint32_t size) {
    memcpy(GetData() + OFFSET_TUPLE_SIZE, &size, sizeof(uint32_t));
  }

  /**
   * Insert a tuple into the table.
   * @param tuple tuple to insert
   * @param[out] rid rid of the inserted tuple
   * @param txn transaction performing the insert
   * @param lock_manager the lock manager
   * @param log_manager the log manager
   * @return true if the insert is successful (i.e. there is enough space)
   */
  auto InsertOverFlowedData(char*data,int size)
      -> bool;

  /**
   * Mark a tuple as deleted. This does not actually delete the tuple.
   * @param rid rid of the tuple to mark as deleted
   * @param txn transaction performing the delete
   * @param lock_manager the lock manager
   * @param log_manager the log manager
   * @return true if marking the tuple as deleted is successful (i.e the tuple exists)
   */
  auto MarkDelete(const RID &rid, Transaction *txn, LockManager *lock_manager, LogManager *log_manager) -> bool;

  /**
   * Update a tuple.
   * @param new_tuple new value of the tuple
   * @param[out] old_tuple old value of the tuple
   * @param rid rid of the tuple
   * @param txn transaction performing the update
   * @param lock_manager the lock manager
   * @param log_manager the log manager
   * @return true if updating the tuple succeeded
   */
  auto UpdateTuple(const Tuple &new_tuple, Tuple *old_tuple, const RID &rid, Transaction *txn,
                   LockManager *lock_manager, LogManager *log_manager) -> bool;

  /** To be called on commit or abort. Actually perform the delete or rollback an insert. */
  void ApplyDelete(const RID &rid, Transaction *txn, LogManager *log_manager);

  /** To be called on abort. Rollback a delete, i.e. this reverses a MarkDelete. */
  void RollbackDelete(const RID &rid, Transaction *txn, LogManager *log_manager);

  /**
   * Read a tuple from a table.
   * @param rid rid of the tuple to read
   * @param[out] tuple the tuple that was read
   * @param txn transaction performing the read
   * @param lock_manager the lock manager
   * @return true if the read is successful (i.e. the tuple exists)
   */
  auto GetTuple(const RID &rid, Tuple *tuple, Transaction *txn, LockManager *lock_manager) -> bool;

  /** @return the rid of the first tuple in this page */

  /**
   * @param[out] first_rid the RID of the first tuple in this page
   * @return true if the first tuple exists, false otherwise
   */
  auto GetTupleRid(RID *first_rid) -> bool;

  auto GetDataSize() -> uint32_t {
    return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_TUPLE_SIZE);
  }
 auto GetTupleData() -> char * {
    return   GetData() + SIZE_OVER_FLOW_HEADER ;
  }
 private:
  static_assert(sizeof(page_id_t) == 4);

  static constexpr size_t SIZE_OVER_FLOW_HEADER = 12;
  static constexpr size_t OFFSET_NEXT_PAGE_ID = 4;
  static constexpr size_t OFFSET_TUPLE_SIZE = 8;
 
 

  // /** @return true if the tuple is deleted or empty */
  // static auto IsDeleted(uint32_t tuple_size) -> bool {
  //   return static_cast<bool>(tuple_size & DELETE_MASK) || tuple_size == 0;
  // }

  // /** @return tuple size with the deleted flag set */
  // static auto SetDeletedFlag(uint32_t tuple_size) -> uint32_t {
  //   return static_cast<uint32_t>(tuple_size | DELETE_MASK);
  // }

  // /** @return tuple size with the deleted flag unset */
  // static auto UnsetDeletedFlag(uint32_t tuple_size) -> uint32_t {
  //   return static_cast<uint32_t>(tuple_size & (~DELETE_MASK));
  // }
};
}  // namespace bustub
