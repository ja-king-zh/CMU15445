//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  //throw NotImplementedException("DeleteExecutor is not implemented");
  child_executor_->Init();
  deleted_ = false;
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (deleted_) {
    return false;
  }
  deleted_ = true;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int cnt = 0;
  while (child_executor_->Next(tuple, rid)) {
    cnt++;
    //将table状态更新为删除
    table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, *rid);
    for (auto index : indexs) {
      auto key = tuple->KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
  }
  std::vector<Value> ans{{TypeId::INTEGER, cnt}};
  *tuple = Tuple(ans, &GetOutputSchema());
  return true;
}

}  // namespace bustub
