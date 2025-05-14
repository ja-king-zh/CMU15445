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
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) ,
aht_(SimpleAggregationHashTable(plan->aggregates_, plan->agg_types_)),
aht_iterator_(aht_.Begin())
{}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    auto key = MakeAggregateKey(&tuple);
    auto value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
  // 如果table是空的, 且聚合函数只有count(*)才需要这么写
  if (aht_iterator_ == aht_.End() && plan_->output_schema_->GetColumnCount() == 1) {  // 空的哈希表也要初始化一遍
    aht_.InsertEmpty();
  }
  aht_iterator_ = aht_.Begin();  // 必须重新初始化一遍
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  auto key = aht_iterator_.Key();
  auto value = aht_iterator_.Val();
  std::vector<Value> res;
  for (auto it : key.group_bys_) {
    res.push_back(it);
  }
  for (auto it : value.aggregates_) {
    res.push_back(it);
  }
  *tuple = Tuple(res, &GetOutputSchema());
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
