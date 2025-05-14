//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
: AbstractExecutor(exec_ctx),
plan_(plan)
{}

void SeqScanExecutor::Init() {
  //throw NotImplementedException("SeqScanExecutor is not implemented");
  auto res = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_ = std::make_unique<TableIterator>(res->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_->IsEnd()) {
    auto item = iter_->GetTuple();
    auto item_meta = item.first;
    auto item_tuple = item.second;
    *tuple = item_tuple;
    *rid = item_tuple.GetRid();
    ++(*iter_);
    if (item_meta.is_deleted_) {
      continue;
    }
    //std::cout << "cnm" << std::endl;
    auto filter_expr = plan_->filter_predicate_;

    if (filter_expr) {
      //std::cout << "cnm2" << std::endl;
      auto value = filter_expr->Evaluate(tuple, GetOutputSchema());
      //std::cout << "cnm3" << std::endl;
      if (value.IsNull() || !value.GetAs<bool>()) {
        continue;
      }
    }

    return true;
  }
  return false;
}

}  // namespace bustub
