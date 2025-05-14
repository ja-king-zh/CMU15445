//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

#include <type/value_factory.h>

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  //throw NotImplementedException("NestIndexJoinExecutor is not implemented");
  child_executor_->Init();
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (child_executor_->Next(tuple, rid)) {
    auto val = plan_->key_predicate_->Evaluate(tuple, child_executor_->GetOutputSchema());
    auto index = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
    Tuple key = Tuple(std::vector<Value>{val}, &index->key_schema_);
    std::vector<RID> res;
    index->index_->ScanKey(key, &res, exec_ctx_->GetTransaction());
    auto table = exec_ctx_->GetCatalog()->GetTable(plan_->inner_table_oid_);
    if (!res.empty()) {
      auto tpl = table->table_->GetTuple(res[0]).second;
      std::vector<Value> vals;
      for (uint32_t j = 0; j < child_executor_->GetOutputSchema().GetColumnCount(); j ++ ) {
        vals.push_back(tuple->GetValue(&child_executor_->GetOutputSchema(), j));
      }
      for (uint32_t j = 0; j < table->schema_.GetColumnCount(); j ++ ) {
        vals.push_back(tpl.GetValue(&table->schema_, j));
      }
      *tuple = Tuple(vals, &GetOutputSchema());
      return true;
    } else {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> vals;
        for (uint32_t j = 0; j < child_executor_->GetOutputSchema().GetColumnCount(); j ++ ) {
          vals.push_back(tuple->GetValue(&child_executor_->GetOutputSchema(), j));
        }
        for (uint32_t j = 0; j < table->schema_.GetColumnCount(); j ++ ) {
          vals.push_back(ValueFactory::GetNullValueByType(table->schema_.GetColumn(j).GetType()));
        }
        *tuple = Tuple(vals, &GetOutputSchema());
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
