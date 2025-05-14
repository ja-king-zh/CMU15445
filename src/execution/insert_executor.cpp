//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  //throw NotImplementedException("InsertExecutor is not implemented");
  child_executor_->Init();
  inserted_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (inserted_) {
    return false;
  }
  inserted_ = true;
  int cnt = 0;
  //通过上下文拿table_info,可通过其插入元组到table,index表示所有建的索引
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  while (child_executor_->Next(tuple, rid)) {
    cnt++;
    //插入当前元组，返回当前元组唯一id
    auto new_rid = table_info->table_->InsertTuple(TupleMeta{0, false}, *tuple);
    for (auto t : indexs) {
      //得到当前元组的key
      auto key = tuple->KeyFromTuple(table_info->schema_, t->key_schema_, t->index_->GetKeyAttrs());
      t->index_->InsertEntry(key, *new_rid, exec_ctx_->GetTransaction());
    }
  }
  std::vector<Value> res={{TypeId::INTEGER, cnt}};
  *tuple=Tuple(res, &GetOutputSchema());
  return true;
}

}  // namespace bustub
