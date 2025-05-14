//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor))
{
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  //throw NotImplementedException("UpdateExecutor is not implemented");
  child_executor_->Init();
  updated_ = false;
  //拿table_info,方便后续插入到table
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (updated_) {
    return false;
  }
  updated_ = true;


  auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Tuple tmp_tuple;
  RID tmp_rid;
  int cnt = 0;
  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    cnt++;
    //把当前元组标记为已删除
    table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, tmp_rid);
    //构造新元组，即更新后的元组
    std::vector<Value> values;
    //values.resize(plan_->target_expressions_.size());
    //std::cout << plan_->target_expressions_.size() << ' ' << child_executor_->GetOutputSchema().ToString() << std::endl;
    for (auto &it : plan_->target_expressions_) {
      values.push_back(it->Evaluate(&tmp_tuple, child_executor_->GetOutputSchema()));
    }
    //将更新后的元组插入到table中
    auto updata_tuple = Tuple{values, &child_executor_->GetOutputSchema()};
    auto new_tid = table_info_->table_->InsertTuple(TupleMeta{0, false}, updata_tuple);
    for (auto index : indexs) {
      //通过tuple拿相应的key
      auto new_key = updata_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      auto old_key = tmp_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      //删除以及添加
      index->index_->DeleteEntry(old_key, tmp_rid, exec_ctx_->GetTransaction());
      index->index_->InsertEntry(new_key, new_tid.value(), exec_ctx_->GetTransaction());
    }
  }
  std::vector<Value> ans{{TypeId::INTEGER,cnt}};
  *tuple = Tuple(ans, &GetOutputSchema());
  return true;
}

}  // namespace bustub
