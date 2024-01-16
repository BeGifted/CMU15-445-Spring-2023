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
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  int32_t num = 0;
  TupleMeta meta = {INVALID_TXN_ID, INVALID_TXN_ID, true};
  while (child_executor_->Next(tuple, rid)) {
    // delete
    table_info_->table_->UpdateTupleMeta(meta, *rid);

    auto table_record = TableWriteRecord(table_info_->oid_, *rid, table_info_->table_.get());
    table_record.wtype_ = WType::DELETE;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(table_record);

    for (auto &index : indexes_) {
      Tuple key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());

      auto index_write_record =
          IndexWriteRecord(*rid, table_info_->oid_, WType::DELETE, key, index->index_oid_, exec_ctx_->GetCatalog());
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(index_write_record);
    }
    num++;
  }
  *tuple = Tuple({{INTEGER, num}}, &GetOutputSchema());
  done_ = true;
  return true;
}

}  // namespace bustub
