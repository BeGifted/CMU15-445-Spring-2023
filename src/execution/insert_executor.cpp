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
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  int32_t num = 0;
  TupleMeta meta = {INVALID_TXN_ID, INVALID_TXN_ID, false};
  while (child_executor_->Next(tuple, rid)) {
    auto inserted_rid = table_info_->table_->InsertTuple(meta, *tuple, exec_ctx_->GetLockManager(),
                                                         exec_ctx_->GetTransaction(), table_info_->oid_);
    if (!inserted_rid.has_value()) {
      continue;
    }
    for (auto &index : indexes_) {
      Tuple key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, inserted_rid.value(), exec_ctx_->GetTransaction());
    }
    num++;
  }
  *tuple = Tuple({{INTEGER, num}}, &GetOutputSchema());
  done_ = true;
  return true;
}

}  // namespace bustub
