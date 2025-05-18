  //===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  //throw NotImplementedException("IndexScanExecutor is not implemented");
  //需要拿到索引开始的迭代器
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  auto btree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  auto key_schema = index_info->key_schema_;
  rids_.clear();
  for (auto key : plan_->pred_keys_) {
    Tuple t;
    auto value = key->Evaluate(&t, key_schema);
    std::vector<Value> values{value};
    Tuple index_key(values, &key_schema);
    btree->ScanKey(index_key, &rids_, exec_ctx_->GetTransaction());
  }
  iter_ = rids_.begin();
  //std::cout << "zheli" << std::endl;
  iter2_ = btree->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->filter_predicate_) {
    //std::cout << "bbbb" << std::endl;
    if (iter_ == rids_.end()) {
      return false;
    }
    while (true) {
      auto temp = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(*iter_);
      *tuple = temp.second;
      *rid = *iter_;
      ++iter_;
      if (temp.first.is_deleted_) {
        continue;
      }
      return true;
    }
  }
  //std::cout << "aaaa" << iter2_.IsEnd() << std::endl;
  if (iter2_.IsEnd()) {
    return false;
  }
  while (true) {
    auto temp = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple((*iter2_).second);
    *tuple = temp.second;
    *rid = (*iter2_).second;
    ++iter2_;
    if (temp.first.is_deleted_) {
      continue;
    }
    return true;
  }
}

}  // namespace bustub
