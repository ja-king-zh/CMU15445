//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <iostream>
#include <optional>
#include <vector>
#include "common/config.h"
#include "execution/plans/sort_plan.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()),  child_executor_(std::move(child_executor)) {}

template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  //throw NotImplementedException("ExternalMergeSortExecutor is not implemented");
  v.clear();
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  int cnt = 0;
  while (child_executor_->Next(&tuple, &rid)) {
    auto key = GenerateSortKey(tuple, plan_->order_bys_, GetOutputSchema());
    v.push_back(SortEntry({key, tuple}));
    cnt++;
    if (cnt == 400) {
      auto pageid = exec_ctx_->GetBufferPoolManager()->NewPage();
      auto pg = exec_ctx_->GetBufferPoolManager()->WritePage(pageid);
      exec_ctx_->GetBufferPoolManager()->FlushAllPages();
      cnt = 0;
    }
  }
  sort(v.begin(), v.end(), cmp_);
  index_ = 0;
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  while (index_ < v.size()) {
    *tuple = v[index_].second;
    *rid = tuple->GetRid();
    index_++;
    return true;
  }
  return false;
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
