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

#include "execution/executors/nested_loop_join_executor.h"

#include <type/value_factory.h>

#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)),
right_executor_(std::move(right_executor)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  //throw NotImplementedException("NestedLoopJoinExecutor is not implemented");
  Tuple tuple;
  RID rid;
  left_executor_->Init();
  right_executor_->Init();
  while (right_executor_->Next(&tuple, &rid)) {
    right_.push_back(tuple);
  }
  joined_ = false;
  right_index_ = 0;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (joined_) {
    //std::cout << "cnm" << std::endl;
    auto left_tuple = left_pre_;
    for (uint32_t i = right_index_ + 1; i < right_.size(); i++) {
      auto value = plan_->Predicate()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
        &right_[i], right_executor_->GetOutputSchema());
      if (value.GetAs<bool>()) {
        std::vector<Value> vals;
        for (uint32_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j ++ ) {
          vals.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), j));
        }
        for (uint32_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j ++ ) {
          vals.push_back(right_[i].GetValue(&right_executor_->GetOutputSchema(), j));
        }
        *tuple = Tuple(vals, &GetOutputSchema());
        right_index_ = i;
        joined_ = true;
        return true;
      }
    }
    right_index_ = 0;
    joined_ = false;
  }
  Tuple left_tuple;
  RID left_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    right_executor_->Init();
    left_pre_ = left_tuple;
    for (uint32_t i = 0; i < right_.size(); i++) {
      auto value = plan_->Predicate()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
        &right_[i], right_executor_->GetOutputSchema());
      if (value.GetAs<bool>()) {
        std::vector<Value> vals;
        for (uint32_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j ++ ) {
          vals.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), j));
        }
        for (uint32_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j ++ ) {
          vals.push_back(right_[i].GetValue(&right_executor_->GetOutputSchema(), j));
        }
        *tuple = Tuple(vals, &GetOutputSchema());
        right_index_ = i;
        joined_ = true;
        return true;
      }
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> vals;
      for (uint32_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j ++ ) {
        vals.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), j));
      }
      for (uint32_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j ++ ) {
        vals.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(j).GetType()));
      }
      *tuple = Tuple(vals, &GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
