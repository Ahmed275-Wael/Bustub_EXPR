// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // nested_loop_join_executor.cpp
// //
// // Identification: src/execution/nested_loop_join_executor.cpp
// //
// // Copyright (c) 2015-2021, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include "execution/executors/nested_loop_join_executor.h"
// #include "binder/table_ref/bound_join_ref.h"
// #include "common/exception.h"
// #include "type/value_factory.h"

// namespace bustub {

// NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
//                                                std::unique_ptr<AbstractExecutor> &&left_executor,
//                                                std::unique_ptr<AbstractExecutor> &&right_executor)
//     : AbstractExecutor(exec_ctx),
//       plan_{plan},
//       left_executor_(std::move(left_executor)),
//       right_executor_(std::move(right_executor)) {
//   if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
//     // Note for 2022 Fall: You ONLY need to implement left join and inner join.
//     throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
//   }
// }
// void NestedLoopJoinExecutor::Init() { 
//       Tuple childTuple;
//       RID childRID;
//       left_executor_->Init();
//       right_executor_->Init();
//       // while (left_executor_->Next(&childTuple, &childRID)) {
//       //   leftTuples.push_back(childTuple);
//       // }

//       left_executor_->Next(&left_tuple_, &childRID);
//       currentLeftTupleIndex = 0;
    
// }

// auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
//   //Note we use while (1) loop as nested loop join is a pipeline breaker!
 
//   bool leftDone = false;
 
//   while(true ) {
//    std::vector<Value> values{};
//   Tuple rightChildTuple;
//   RID rightChildRID;
//   RID leftChildRID;
//   // if (currentLeftTupleIndex >= leftTuples.size()) return false;
//   if (plan_->GetJoinType() == JoinType::INNER) {
//     if (right_executor_->Next(&rightChildTuple, &rightChildRID)) {
//       auto ret = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &rightChildTuple,
//                                                right_executor_->GetOutputSchema());
//       if (!ret.IsNull() && ret.GetAs<bool>())  {
//       JoinTuples(rightChildTuple, values);
//       *tuple = Tuple{values, &GetOutputSchema()};
//       *rid = tuple->GetRid();
//       return true;
//       }
//       continue;
//     }
//     //Here means no right element suitable to our current left
//       right_executor_->Init(); // Move the cursor again to first element in the right table
//       // currentLeftTupleIndex++;
//       bool ret = left_executor_->Next(&left_tuple_, &leftChildRID);

//       continue;
//   } else if (plan_->GetJoinType() == JoinType::LEFT) {
//       if (right_executor_->Next(&rightChildTuple, &rightChildRID)) {
//       auto ret = plan_->Predicate().EvaluateJoin(&leftTuples[currentLeftTupleIndex], left_executor_->GetOutputSchema(), &rightChildTuple,
//                                                right_executor_->GetOutputSchema());
//       if (ret.IsNull() || !ret.GetAs<bool>()) continue;
//       JoinTuples(rightChildTuple, values);
//       *tuple = Tuple{values, &GetOutputSchema()};
//       *rid = tuple->GetRid();
//       currentLeftTupleIndex++;
//       leftDone = true;
//       right_executor_->Init();  // Move the cursor again to first element in the right table
//       return true;
//     }
//     if (!leftDone) {
//       //we need to insert nulls to the left tuple
//       *tuple = MergeWithNulls();
//       *rid = tuple->GetRid();
//     }
//     currentLeftTupleIndex++;
//     right_executor_->Init(); 
//     leftDone = false;
//     return true;
//   }

//   }
  
// }

// void NestedLoopJoinExecutor::JoinTuples(Tuple & rightChildTuple, std::vector<Value>&values) {
//      for (int i = 0 ; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {  
//         values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
//       }
//       for (int i = 0 ; i <right_executor_->GetOutputSchema().GetColumnCount(); i++) {  
//         values.push_back(rightChildTuple.GetValue(&right_executor_->GetOutputSchema(), i));
//       }
// }

// auto NestedLoopJoinExecutor::MergeWithNulls() -> Tuple {
//   std::vector<Value> values{};
//   values.reserve(GetOutputSchema().GetColumnCount());

//   for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
//     values.push_back(leftTuples[currentLeftTupleIndex].GetValue(&left_executor_->GetOutputSchema(), idx));
//   }
//   for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
//     values.push_back(ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(idx).GetType()));
//   }

//   return Tuple{values, &GetOutputSchema()};
// }
// }  // namespace bustub
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// #include "execution/executors/nested_loop_join_executor.h"
// #include "binder/table_ref/bound_join_ref.h"
// #include "common/exception.h"
// #include "type/value_factory.h"

// namespace bustub {

// NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
//                                                std::unique_ptr<AbstractExecutor> &&left_executor,
//                                                std::unique_ptr<AbstractExecutor> &&right_executor)
//     : AbstractExecutor(exec_ctx),
//       plan_{plan},
//       left_executor_(std::move(left_executor)),
//       right_executor_(std::move(right_executor)) {
//   if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
//     // Note for 2022 Fall: You ONLY need to implement left join and inner join.
//     throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
//   }
// }

// void NestedLoopJoinExecutor::Init() {
//   left_executor_->Init();
//   right_executor_->Init();
//   Tuple tuple{};
//   RID rid{};
//   while (right_executor_->Next(&tuple, &rid)) {
//     right_tuples_.push_back(tuple);
//   }
// }

// auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   RID emit_rid{};
//   while (right_tuple_idx_ >= 0 || left_executor_->Next(&left_tuple_, &emit_rid)) {
//     std::vector<Value> vals;
//     for (uint32_t ridx = (right_tuple_idx_ < 0 ? 0 : right_tuple_idx_); ridx < right_tuples_.size(); ridx++) {
//       auto &right_tuple = right_tuples_[ridx];
//       if (Matched(&left_tuple_, &right_tuple)) {
//         for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
//           vals.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), idx));
//         }
//         for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
//           vals.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), idx));
//         }
//         *tuple = Tuple(vals, &GetOutputSchema());
//         right_tuple_idx_ = ridx + 1;
//         return true;
//       }
//     }
//     if (right_tuple_idx_ == -1 && plan_->GetJoinType() == JoinType::LEFT) {
//       for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
//         vals.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), idx));
//       }
//       for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
//         vals.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(idx).GetType()));
//       }
//       *tuple = Tuple(vals, &GetOutputSchema());
//       return true;
//     }
//     right_tuple_idx_ = -1;
//   }
//   return false;
// }

// auto NestedLoopJoinExecutor::Matched(Tuple *left_tuple, Tuple *right_tuple) const -> bool {
//   auto value = plan_->Predicate().EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple,
//                                                right_executor_->GetOutputSchema());

//   return !value.IsNull() && value.GetAs<bool>();
// }

// }  // namespace bustub

// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // nested_loop_join_executor.cpp
// //
// // Identification: src/execution/nested_loop_join_executor.cpp
// //
// // Copyright (c) 2015-2021, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}
void NestedLoopJoinExecutor::Init() { 
      Tuple childTuple;
      RID childRID;
      left_executor_->Init();
      right_executor_->Init();
      while (left_executor_->Next(&childTuple, &childRID)) {
        leftTuples.push_back(childTuple);
      }
    
       
      // currentLeftTupleIndex = 0;
      //Line 251 cost me 5 hours to catch that its bug :D  (as the parent might be join if he use init as in the next it will reset the currentLeftTuple causing many many duplicates)
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  //Note we use while (1) loop as nested loop join is a pipeline breaker!
 
  
  while(true ) {
   std::vector<Value> values{};
  Tuple rightChildTuple;
  RID rightChildRID;
  RID leftChildRID;
  if ((size_t)currentLeftTupleIndex >= leftTuples.size()) return false;
  if (plan_->GetJoinType() == JoinType::INNER) {
 
    if (right_executor_->Next(&rightChildTuple, &rightChildRID)) {
      auto ret = plan_->Predicate().EvaluateJoin(&leftTuples[currentLeftTupleIndex], left_executor_->GetOutputSchema(), &rightChildTuple,
                                               right_executor_->GetOutputSchema());
      if (!ret.IsNull() && ret.GetAs<bool>())  {
      JoinTuples(rightChildTuple, values);
      *tuple = Tuple{values, &GetOutputSchema()};
      *rid = tuple->GetRid();
      return true;
      }
      continue;
    }
    //Here means no right element suitable to our current left
      right_executor_->Init(); // Move the cursor again to first element in the right table
      currentLeftTupleIndex++;
  
      continue;
  } else if (plan_->GetJoinType() == JoinType::LEFT) {
      if (right_executor_->Next(&rightChildTuple, &rightChildRID)) {
      auto ret = plan_->Predicate().EvaluateJoin(&leftTuples[currentLeftTupleIndex], left_executor_->GetOutputSchema(), &rightChildTuple,
                                               right_executor_->GetOutputSchema());
      if (ret.IsNull() || !ret.GetAs<bool>()) continue;
      JoinTuples(rightChildTuple, values);
      *tuple = Tuple{values, &GetOutputSchema()};
      *rid = tuple->GetRid();
    
      leftDone = true;
    
      return true;
    }
    if (!leftDone) {
      //we need to insert nulls to the left tuple
      *tuple = MergeWithNulls();
      *rid = tuple->GetRid();
       currentLeftTupleIndex++;
       right_executor_->Init(); 
       leftDone = false;
       return true;
    } else {
      leftDone = false;
      currentLeftTupleIndex++;
       right_executor_->Init(); 
       continue;
    }
    
  }

  }
  
}

void NestedLoopJoinExecutor::JoinTuples(Tuple & rightChildTuple, std::vector<Value>&values) {
     for (size_t i = 0 ; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {  
        values.push_back(leftTuples[currentLeftTupleIndex].GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (size_t i = 0 ; i <right_executor_->GetOutputSchema().GetColumnCount(); i++) {  
        values.push_back(rightChildTuple.GetValue(&right_executor_->GetOutputSchema(), i));
      }
}

auto NestedLoopJoinExecutor::MergeWithNulls() -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(leftTuples[currentLeftTupleIndex].GetValue(&left_executor_->GetOutputSchema(), idx));
  }
  for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(idx).GetType()));
  }

  return Tuple{values, &GetOutputSchema()};
}
}  // namespace bustub
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//