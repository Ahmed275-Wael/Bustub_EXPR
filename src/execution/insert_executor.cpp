//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)), plan_(plan) {}

void InsertExecutor::Init() { 
     child_executor_->Init();
      noOfCalls = 0;
};

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    
   table_oid_t tableId =  this->plan_->TableOid();  
   
   ExecutorContext *exec_ctx = this->GetExecutorContext();
   Catalog *catalog = exec_ctx->GetCatalog();
   TableInfo*tableInfo = catalog->GetTable(tableId);
    TableHeap *tableHeap = tableInfo->table_.get();
    Tuple childTuple;
    RID childRid;
    int size = 0;
        std::vector<IndexInfo *> indexInfos = catalog->GetTableIndexes(tableInfo->name_);
     //Inserting The Tuples
     while(child_executor_->Next(&childTuple, &childRid)) {
        LOG_DEBUG("5555555555The Size of Tuple is %d" , childTuple.GetLength());
 
        bool result =  tableHeap->InsertTuple(childTuple,&childRid,exec_ctx->GetTransaction());
          //Checking for available index and updating them
         if (!result) {
               continue;
         }
              for (size_t i = 0; i < indexInfos.size(); i++) {
                Index * index = indexInfos[i]->index_.get();
                index->InsertEntry(childTuple.KeyFromTuple(tableInfo->schema_, indexInfos[i]->key_schema_, indexInfos[i]->index_->GetKeyAttrs()), childRid, exec_ctx->GetTransaction());
         }
        size++;
     }
    
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());  //1
    values.push_back(Value(INTEGER, size));
    *tuple = Tuple{values, &GetOutputSchema()};
    noOfCalls++;
    if (size == 0 && noOfCalls == 1) return true;
    return size != 0;
 }

}  // namespace bustub
 