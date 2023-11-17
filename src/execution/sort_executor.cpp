#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)), plan_(plan) {}

void SortExecutor::Init() { 
    child_executor_->Init();
    tuples.clear();
    Tuple childTuple;
    RID childRid;
    while (child_executor_->Next(&childTuple, &childRid)) {
        tuples.push_back(childTuple);
    }
auto cmpLambda = [orderByArr = plan_->GetOrderBy(),childSchema = child_executor_->GetOutputSchema() ](Tuple &a, Tuple &b) {
 

    for (size_t i = 0; i < orderByArr.size(); i++) {
        OrderByType orderType = orderByArr[i].first;

        if (orderType == OrderByType::ASC || orderType == OrderByType::DEFAULT) {
            if (static_cast<bool>(orderByArr[i].second->Evaluate(&a, childSchema).CompareEquals(orderByArr[i].second->Evaluate(&b, childSchema))))
                continue;

            return static_cast<bool>(orderByArr[i].second->Evaluate(&a, childSchema).CompareLessThan(orderByArr[i].second->Evaluate(&b, childSchema)));
        } else if (orderType == OrderByType::DESC) {
            if (static_cast<bool>(orderByArr[i].second->Evaluate(&a, childSchema).CompareEquals(orderByArr[i].second->Evaluate(&b, childSchema))))
                continue;

            return !static_cast<bool>(orderByArr[i].second->Evaluate(&a, childSchema).CompareLessThan(orderByArr[i].second->Evaluate(&b, childSchema)));
        }
    }

    return false;
};
    std::sort(tuples.begin(), tuples.end(), cmpLambda);
    std::vector<Tuple>::iterator iter;
    iter = tuples.begin();
    iter_ = iter;
 }

// bool SortExecutor::cmp(Tuple *a, Tuple * b) {
//     auto orderByArr  = plan_->GetOrderBy();
//     Schema   childSchema = child_executor_->GetOutputSchema();
//     for (int i = 0; i < orderByArr.size(); i++) {
//        OrderByType orderType =  orderByArr[i].first;
//        if (orderType == OrderByType::ASC || orderType == OrderByType::DEFAULT) {
//             if (static_cast<bool>(orderByArr[i].second->Evaluate(a, childSchema).CompareEquals(orderByArr[i].second->Evaluate(b, childSchema)))) continue;
//             return static_cast<bool>(orderByArr[i].second->Evaluate(a, childSchema).CompareLessThan(orderByArr[i].second->Evaluate(b,  childSchema)));
//        } else if (orderType == OrderByType::DESC) {
//          if (static_cast<bool>(orderByArr[i].second->Evaluate(a, childSchema).CompareEquals(orderByArr[i].second->Evaluate(b, childSchema)))) continue;
//             return !static_cast<bool>(orderByArr[i].second->Evaluate(a, childSchema).CompareLessThan(orderByArr[i].second->Evaluate(b, childSchema)));
//        }
//     }
//     return false;
// }
auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {  
    if (iter_ == tuples.end()) return false;
    *tuple = *iter_;
    *rid = tuple->GetRid();
    ++iter_;
    return true;

 }

}  // namespace bustub
