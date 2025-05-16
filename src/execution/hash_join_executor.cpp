//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

#include <type/value_factory.h>

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(std::move(left_child)), right_child_(std::move(right_child)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Fall 2024: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  //throw NotImplementedException("HashJoinExecutor is not implemented");
  left_child_->Init();
  right_child_->Init();
  has_joined_ = false;
  right_index = 0;
  mp_ = std::make_unique<SimpleHashJoinTable>();
  Tuple tuple;
  RID rid;
  while (right_child_->Next(&tuple, &rid)) {
    mp_->Insert(MakeRightJoinKey(&tuple), tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_joined_) {
    //std::cout << "cnm2" << std::endl;
    auto left = pre_left_tuple;
    right_index++;
    //std::cout << mp_->GetValue(MakeLeftJoinKey(&left)).aggregates_.size() << ' ' << right_index << std::endl;
    if (mp_->GetValue(MakeLeftJoinKey(&left)).aggregates_.size() > right_index) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i ++ ) {
        values.push_back(left.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i ++ ) {
        values.push_back(mp_->GetValue(MakeLeftJoinKey(&left)).aggregates_[right_index].GetValue(&right_child_->GetOutputSchema(), i));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
  }
  //std::cout << "cnm" << std::endl;
  Tuple left_tuple;
  RID left_rid;
  while (left_child_->Next(&left_tuple, &left_rid)) {
    pre_left_tuple = left_tuple;
    auto key = MakeLeftJoinKey(&left_tuple);
    if (!mp_->GetValue(key).aggregates_.empty()) {
      //std::cout << "cnm3" << std::endl;
      has_joined_ = true;
      right_index = 0;
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i ++ ) {
        values.push_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i ++ ) {
        values.push_back(mp_->GetValue(key).aggregates_[right_index].GetValue(&right_child_->GetOutputSchema(), i));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
    if (plan_->join_type_ == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i ++ ) {
        values.push_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i ++ ) {
        values.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
