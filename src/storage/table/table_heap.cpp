//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// table_heap.cpp
//
// Identification: src/storage/table/table_heap.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>

#include "common/logger.h" 
#include "fmt/format.h"
#include "storage/table/table_heap.h"
#include "storage/page/over_flow_page.h"
namespace bustub {

TableHeap::TableHeap(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager, LogManager *log_manager,
                     page_id_t first_page_id)
    : buffer_pool_manager_(buffer_pool_manager),
      lock_manager_(lock_manager),
      log_manager_(log_manager),
      first_page_id_(first_page_id) {
  auto first_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  //RID rid;
  //&& first_page->GetFirstTupleRid(&rid) == true
  BUSTUB_ASSERT(first_page != nullptr ,
                "Couldn't retrieve a page for the table heap");
  //std::cerr<<"RRRRRRRRIIIIIIIIIIDDDDDDDD"<<rid.ToString();
  //first_page->Init(first_page_id_, BUSTUB_PAGE_SIZE, INVALID_LSN, log_manager_, nullptr);
  buffer_pool_manager_->UnpinPage(first_page_id_, true);    
      }

TableHeap::TableHeap(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager, LogManager *log_manager,
                     Transaction *txn)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  // Initialize the first table page.
  auto first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(&first_page_id_));
  BUSTUB_ASSERT(first_page != nullptr,
                "Couldn't create a page for the table heap. Have you completed the buffer pool manager project?");
  first_page->Init(first_page_id_, BUSTUB_PAGE_SIZE, INVALID_LSN, log_manager_, txn);
  buffer_pool_manager_->UnpinPage(first_page_id_, true);
}

auto TableHeap::InsertTuple(const Tuple &tuple, RID *rid, Transaction *txn) -> bool {
 
  auto cur_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (cur_page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }


  if (tuple.size_ + 32 > BUSTUB_PAGE_SIZE) { 
    //  LOG_DEBUG("Overflowing...."); // larger than one page size
    // txn->SetState(TransactionState::ABORTED); 
    page_id_t over_flow_id;
    auto overFlowPage = static_cast<OverFlowPage *>(buffer_pool_manager_->NewPage(&over_flow_id));
      if (overFlowPage == nullptr) {
        txn->SetState(TransactionState::ABORTED);
        return false;
      }
      //SHOULD I LOOK FOR THE LAST PAGE OR I MAKE TUPLE CAN BE WITH AVAILABLE SIZE AND AN OVERFLOW ? (THIS DECISSION AFFECT WHETHER OVERFLOWID IN IN TUPLE LIKE SQLITE) OR NOT LOOKHERE
      //Searching for the last page in the table_heap (SEQUENTIAL FILE ORGANIZATION SUCKS!!!)
      cur_page->WLatch();
      overFlowPage->WLatch();
      auto next_page_id = cur_page->GetNextPageId();
      while (next_page_id != INVALID_PAGE_ID) {
      auto next_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
      next_page->WLatch();
      buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), false);
      cur_page->WUnlatch();
      cur_page = next_page;
      next_page_id = cur_page->GetNextPageId();
      }
      auto new_page = static_cast<TablePage *>(buffer_pool_manager_->NewPage(&next_page_id));
      if (new_page == nullptr) {
        // Then life sucks and we abort the transaction.
        cur_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), false);
        txn->SetState(TransactionState::ABORTED);
        return false;
      }
      new_page->WLatch();
      cur_page->SetNextPageId(next_page_id);
      new_page->Init(next_page_id, BUSTUB_PAGE_SIZE, cur_page->GetTablePageId(), log_manager_, txn);
      overFlowPage->Init(over_flow_id, BUSTUB_PAGE_SIZE, INVALID_PAGE_ID, log_manager_, txn);
      cur_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), true);

      //Split the Tuple 
       Tuple leftTuple;
       size_t tupleInPageSize = BUSTUB_PAGE_SIZE - 32; //this 32 for slotted page metadata (why i am aware of this in the heap? LOOKHERE)
       char*overFlowedData;
       SplitData(tuple.data_, tuple.size_, &leftTuple.data_, tupleInPageSize, &overFlowedData, tuple.size_ - tupleInPageSize);
      //  LOG_DEBUG("First Page Size = %d", (int)tupleInPageSize);
        // LOG_DEBUG("Second Page Size = %d", (int)(tuple.size_ - tupleInPageSize));
         
   
       leftTuple.rid_ = tuple.rid_;
       leftTuple.allocated_ = tuple.allocated_;
       leftTuple.size_ = tupleInPageSize;
       leftTuple.SetOverFlowPageId(over_flow_id);
       
       bool result =  new_page->InsertTuple(leftTuple, rid, txn, lock_manager_, log_manager_);
       new_page->WUnlatch();
       buffer_pool_manager_->UnpinPage(next_page_id, true);
       auto overFlowedDataSize = tuple.size_ - tupleInPageSize;
       //Here we will check if the overFlowedData need another overFlowPages?
       while (overFlowedDataSize > 0) {
          if (overFlowedDataSize > BUSTUB_PAGE_SIZE - 12) {
            page_id_t over_flow_id;
            auto newOverFlowPage = static_cast<OverFlowPage *>(buffer_pool_manager_->NewPage(&over_flow_id));
            newOverFlowPage->Init(over_flow_id, BUSTUB_PAGE_SIZE, INVALID_PAGE_ID, log_manager_, txn);
            char *overFlowedTemp;
            char * overFlowRemain;
            SplitData(overFlowedData, overFlowedDataSize, &overFlowedTemp, BUSTUB_PAGE_SIZE - 12, &overFlowRemain, overFlowedDataSize - (BUSTUB_PAGE_SIZE - 12));
            overFlowPage->SetNextPageId(over_flow_id);
            overFlowPage->SetTupleSize(BUSTUB_PAGE_SIZE - 12);
            overFlowPage->InsertOverFlowedData(overFlowedTemp, BUSTUB_PAGE_SIZE - 12);
            overFlowPage->WUnlatch();
            buffer_pool_manager_->UnpinPage(over_flow_id, true);
            delete []overFlowedData;
            delete[]overFlowedTemp;
            overFlowedData = overFlowRemain;
            overFlowedDataSize -= (BUSTUB_PAGE_SIZE - 12);
            buffer_pool_manager_->UnpinPage(overFlowPage->GetPageId(), true);
            overFlowPage = newOverFlowPage;
            overFlowPage->WLatch();
          } else {
           overFlowPage->InsertOverFlowedData(overFlowedData, overFlowedDataSize);
           overFlowPage->SetTupleSize(overFlowedDataSize);
           overFlowPage->WUnlatch(); //Pin l awl wla unLatch ana nasy (LOOKHERE)
           buffer_pool_manager_->UnpinPage(overFlowPage->GetPageId(), true);
           overFlowedDataSize = 0;
           delete[] overFlowedData;
          }

       }
   
       txn->GetWriteSet()->emplace_back(*rid, WType::INSERT, Tuple{}, this);
       
       return result;
  } 

  cur_page->WLatch();

  // Insert into the first page with enough space. If no such page exists, create a new page and insert into that.
  // INVARIANT: cur_page is WLatched if you leave the loop normally.
  while (!cur_page->InsertTuple(tuple, rid, txn, lock_manager_, log_manager_)) {
    auto next_page_id = cur_page->GetNextPageId();
    // If the next page is a valid page,
    if (next_page_id != INVALID_PAGE_ID) {
      auto next_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
      next_page->WLatch();
      // Unlatch and unpin the current page.
      cur_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), false);
      cur_page = next_page;
    } else {
      // Otherwise we have run out of valid pages. We need to create a new page.
      auto new_page = static_cast<TablePage *>(buffer_pool_manager_->NewPage(&next_page_id));
      // If we could not create a new page,
      if (new_page == nullptr) {
        // Then life sucks and we abort the transaction.
        cur_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), false);
        txn->SetState(TransactionState::ABORTED);
        return false;
      }
      // Otherwise we were able to create a new page. We initialize it now.
      new_page->WLatch();
      cur_page->SetNextPageId(next_page_id);
      new_page->Init(next_page_id, BUSTUB_PAGE_SIZE, cur_page->GetTablePageId(), log_manager_, txn);
      cur_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), true);
      cur_page = new_page;
    }
  }
  // This line has caused most of us to double-take and "whoa double unlatch".
  // We are not, in fact, double unlatching. See the invariant above.
  cur_page->WUnlatch(); //by3ml unlatch ll newpage yo3tbr
  buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), true);
  // Update the transaction's write set.
  txn->GetWriteSet()->emplace_back(*rid, WType::INSERT, Tuple{}, this);
  return true;
}

auto TableHeap::MarkDelete(const RID &rid, Transaction *txn) -> bool {
  // TODO(Amadou): remove empty page
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  // Update the transaction's write set.
  txn->GetWriteSet()->emplace_back(rid, WType::DELETE, Tuple{}, this);
  return true;
}

auto TableHeap::UpdateTuple(const Tuple &tuple, const RID &rid, Transaction *txn) -> bool {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // Update the tuple; but first save the old value for rollbacks.
  Tuple old_tuple;
  page->WLatch();
  bool is_updated = page->UpdateTuple(tuple, &old_tuple, rid, txn, lock_manager_, log_manager_);
  uint32_t freeSpace = page->GetFreeSpaceRemaining();
  page->WUnlatch();
  if (is_updated == false && freeSpace + old_tuple.GetLength() < tuple.GetLength()) {
    bool isMarked =  this->MarkDelete(rid,txn);
     RID insertedRid;
     if (!isMarked) {
          txn->SetState(TransactionState::ABORTED);
           return false;
     }
     is_updated = this->InsertTuple(tuple, &insertedRid, txn);
     LOG_DEBUG("ISSSSSS uPDATED %d", is_updated);
  }
  
   
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), is_updated);
  // Update the transaction's write set.
  if (is_updated && txn->GetState() != TransactionState::ABORTED) {
    txn->GetWriteSet()->emplace_back(rid, WType::UPDATE, old_tuple, this);
  }
  return is_updated;
}

void TableHeap::ApplyDelete(const RID &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  BUSTUB_ASSERT(page != nullptr, "Couldn't find a page containing that RID.");
  // Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  /** Commented out to make compatible with p4; This is called only on commit or delete, which consequently unlocks the
   * tuple; so should be fine */
  // lock_manager_->Unlock(txn, rid);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RID &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  BUSTUB_ASSERT(page != nullptr, "Couldn't find a page containing that RID.");
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}
char* TableHeap::MergeData( char* ptr1, uint32_t size1,  char* ptr2, uint32_t size2) {
    // Calculate the size of the new buffer
    size_t newSize = size1 + size2;
 
    // Allocate memory for the new buffer
    char* mergedBuffer = new char[newSize];

    // Copy data from the first pointer to the new buffer
    std::memcpy(mergedBuffer, ptr1, size1);

    // Copy data from the second pointer to the new buffer
    std::memcpy(mergedBuffer + size1, ptr2, size2);
  //   LOG_DEBUG("size 1 is %d, size 2 is %d", size1,size2);
  //  for (uint32_t i = 0; i < newSize - 1; i++) {
  //             LOG_DEBUG("%c", mergedBuffer[i]);
  //           }
    return mergedBuffer;
}

void TableHeap::SplitData(char*data, int length ,char**dest1,int length1,char**dest2,int length2){
       (*dest1) = new char[length1];
        (*dest2) = new char[length2];
       std::memcpy(*dest1, data, length1);
       std::memcpy(*dest2, data + length1, length2);
} 
auto TableHeap::GetTuple(const RID &rid, Tuple *tuple, Transaction *txn, bool acquire_read_lock) -> bool {
  // Find the page which contains the tuple.
 
  auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // Read the tuple from the page.
  if (acquire_read_lock) {
    page->RLatch();
  }
  bool res = page->GetTuple(rid, tuple, txn, lock_manager_);
        //  LOG_DEBUG("The OverFlowPageId  in GetTuple is %d" , tuple->GetOverFlowPageId());
  if (tuple->GetOverFlowPageId() != INVALID_PAGE_ID) {
     auto overFlowPage = static_cast<OverFlowPage *>(buffer_pool_manager_->FetchPage(tuple->GetOverFlowPageId()));
     
    //  LOG_DEBUG("1162023 %d", tuple->GetLength());
    //  LOG_DEBUG("xD %d", overFlowPage->GetDataSize());
    //  LOG_DEBUG("xD5555 %d", tuple->GetLength() + overFlowPage->GetDataSize());

      char *mergedData =   MergeData(tuple->data_, tuple->GetLength(), overFlowPage->GetTupleData(), overFlowPage->GetDataSize());
      delete [] tuple->data_;
      tuple->data_ = mergedData;
      //  LOG_DEBUG("YBDANY");
      tuple->size_ = tuple->GetLength() + overFlowPage->GetDataSize();
      page_id_t nextOverFlowId = overFlowPage->GetNextPageId();
      buffer_pool_manager_->UnpinPage(overFlowPage->GetPageId(), false);
      while (nextOverFlowId != INVALID_PAGE_ID) {
          auto overFlowPageChild = static_cast<OverFlowPage *>(buffer_pool_manager_->FetchPage(nextOverFlowId));
          assert (overFlowPageChild != nullptr);

          // LOG_DEBUG("Merging size of %d with %d", tuple->GetLength(), overFlowPageChild->GetDataSize());
          char *childMergedData =   MergeData(tuple->data_, tuple->GetLength(), overFlowPageChild->GetTupleData(), overFlowPageChild->GetDataSize());
           delete [] tuple->data_;
           tuple->data_ = childMergedData;
           tuple->size_ +=  overFlowPageChild->GetDataSize();
           nextOverFlowId = overFlowPageChild->GetNextPageId();
           buffer_pool_manager_->UnpinPage(overFlowPageChild->GetPageId(), false);
          
      }

  }


  if (acquire_read_lock) {
    page->RUnlatch();
  }
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
  return res;
}
 
auto TableHeap::Begin(Transaction *txn) -> TableIterator {
  // Start an iterator from the first page.
  // TODO(Wuwen): Hacky fix for now. Removing empty pages is a better way to handle this.
  RID rid;
  auto page_id = first_page_id_;
  while (page_id != INVALID_PAGE_ID) {
    LOG_DEBUG("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS\n");
    auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    page->RLatch();
    // If this fails because there is no tuple, then RID will be the default-constructed value, which means EOF.
    auto found_tuple = page->GetFirstTupleRid(&rid);
    std::cerr<<rid.ToString()<<" page_id: "<<page_id;
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, false);
    if (found_tuple) {
      break;
    }
    page_id = page->GetNextPageId();
  }
  return {this, rid, txn};
}

auto TableHeap::End() -> TableIterator { return {this, RID(INVALID_PAGE_ID, 0), nullptr}; }

}  // namespace bustub
