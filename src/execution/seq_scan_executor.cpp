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
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      it_(std::make_unique<TableIterator>(table_info_->table_->MakeEagerIterator())) {}

void SeqScanExecutor::Init() {
  try {
    if (exec_ctx_->IsDelete()) {
      // 无论隔离级别如何，事务都应为所有写操作加 X 锁，直到提交或终止
      if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                  LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_)) {
        throw ExecutionException("lock table INTENTION_EXCLUSIVE fail");
      }
    } else if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(table_info_->oid_)) {
        if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                    LockManager::LockMode::INTENTION_SHARED, table_info_->oid_)) {
          throw ExecutionException("lock table INTENTION_SHARED fail");
        }
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException(e.GetInfo());
  }
  it_ = std::make_unique<TableIterator>(table_info_->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  //  while (!it_.IsEnd()) {
  //    *rid = it_.GetRID();
  //    auto [meta, new_tuple] = it_.GetTuple();
  //    ++it_;
  //    if (!meta.is_deleted_) {
  //      *tuple = new_tuple;
  //      return true;
  //    }
  //  }
  //  return false;
  // 1. 获取表迭代器的当前位置
  while (!it_->IsEnd()) {
    *rid = it_->GetRID();
    // 2. 根据隔离级别的需要锁定元组
    try {
      if (exec_ctx_->IsDelete()) {
        if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                  plan_->table_oid_, *rid)) {
          throw ExecutionException("lock row EXCLUSIVE fail");
        }
      } else if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        if (!exec_ctx_->GetTransaction()->IsRowExclusiveLocked(table_info_->oid_, *rid)) {
          if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                    plan_->table_oid_, *rid)) {
            throw ExecutionException("lock row SHARED fail");
          }
        }
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException(e.GetInfo());
    }

    // 3. 抓取元组。检查元组元，如果已执行过滤推送扫描，则检查谓词
    auto [meta, new_tuple] = it_->GetTuple();
    ++(*it_);
    bool can_read = !meta.is_deleted_;
    if (!meta.is_deleted_ && plan_->filter_predicate_) {
      auto value = plan_->filter_predicate_->Evaluate(&new_tuple, GetOutputSchema());
      can_read = !value.IsNull() && value.GetAs<bool>();
    }

    // 4. 如果元组不应被该事务读取，则强制解锁该行。否则，根据隔离级别的需要解锁该行。
    if (!can_read) {
      if (exec_ctx_->IsDelete() ||
          exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        try {
          if (!exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), table_info_->oid_, *rid, true)) {
            throw ExecutionException("unlock row fail");
          }
        } catch (TransactionAbortException &e) {
          throw ExecutionException(e.GetInfo());
        }
      }
    } else {
      *tuple = new_tuple;
      // 对于 READ_COMMITTED，事务应为所有读操作加 S 锁，但可以立即释放
      if (!exec_ctx_->IsDelete() &&
          exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        try {
          if (!exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), table_info_->oid_, *rid)) {
            throw ExecutionException("unlock row fail");
          }
        } catch (TransactionAbortException &e) {
          throw ExecutionException(e.GetInfo());
        }
      }
      return true;
    }
  }

  // 对于 READ_COMMITTED，事务应为所有读操作加 S 锁，但可以立即释放。
  if (!exec_ctx_->IsDelete() && exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    try {
      if (!exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), plan_->table_oid_)) {
        throw ExecutionException("unlock row fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException(e.GetInfo());
    }
  }
  return false;
}

}  // namespace bustub
