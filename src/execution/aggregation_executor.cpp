//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)), aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()) , aht_iterator_(aht_.Begin()) {
   
    }

void AggregationExecutor::Init() {
       aht_.Clear();
       child_->Init();
       Tuple childTuple;
       RID  childRid;
 ;
        while (child_->Next(&childTuple, &childRid)) {
           
            aht_.InsertCombine(MakeAggregateKey(&childTuple), MakeAggregateValue(&childTuple));
        }
     // Although i define this in the constructor but because of the resizeable property of container in cpp the memory location can be changed 
      aht_iterator_ = aht_.Begin();
      //If We Do Nothing So generate initial values (note the getGroup size as if there is groupBy and no result we must return nothhing not null ;)
      if (aht_iterator_ == aht_.End() && plan_->GetGroupBys().size() == 0) {
      aht_.Insert({}, aht_.GenerateInitialAggregateValue());
       aht_iterator_ = aht_.Begin();
      }
      
}   

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
       if (aht_iterator_ == aht_.End()) return false;
        std::vector<Value> values{};
        values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
        values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
         
        *tuple = Tuple{values, &GetOutputSchema()};
    
        *rid = tuple->GetRid();
        ++aht_iterator_;
        return true;

 }

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
