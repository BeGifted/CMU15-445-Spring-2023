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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  int32_t num = 0;
  TupleMeta meta = {INVALID_TXN_ID, INVALID_TXN_ID, true};
  while (child_executor_->Next(tuple, rid)) {
    // delete
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, *rid);
    for (auto &index : indexes_) {
      Tuple key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }

    // update tuple
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple = Tuple(values, &child_executor_->GetOutputSchema());

    // insert
    meta.is_deleted_ = false;
    auto inserted_rid = table_info_->table_->InsertTuple(meta, new_tuple, exec_ctx_->GetLockManager(),
                                                         exec_ctx_->GetTransaction(), table_info_->oid_);
    if (!inserted_rid.has_value()) {
      continue;
    }
    for (auto &index : indexes_) {
      Tuple key = new_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, inserted_rid.value(), exec_ctx_->GetTransaction());
    }
    num++;
  }
  *tuple = Tuple({{INTEGER, num}}, &GetOutputSchema());
  done_ = true;
  return true;
}

}  // namespace bustub
