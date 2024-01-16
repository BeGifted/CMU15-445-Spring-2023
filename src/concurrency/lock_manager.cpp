//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // ISOLATION LEVEL
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn->GetState() == TransactionState::SHRINKING &&
          (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
          lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
  }

  // get LockRequestQueue
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  std::unique_lock lock(lock_request_queue->latch_);

  // lock upgrade
  //  for (const std::shared_ptr<LockRequest> &request : lock_request_queue->request_queue_) {
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
    auto request = *it;
    if (request->txn_id_ != txn->GetTransactionId()) {
      continue;
    }

    // it already has the lock
    if (request->lock_mode_ == lock_mode) {
      return true;
    }

    // Lock() should upgrade the lock held by the transaction
    // Multiple concurrent lock upgrades on the same resource
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }

    // can lock upgrade?
    if (!CanLockUpgrade(request->lock_mode_, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    // if possible, then drop current lock
    lock_request_queue->request_queue_.erase(it);
    DeleteTableLockSet(txn, request);

    auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    // find first location where not granted
    // auto itr = std::find_if(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
    //                         [](const std::shared_ptr<LockRequest> &request) { return !request->granted_; });
    // lock_request_queue->request_queue_.insert(itr, upgrade_lock_request);
    lock_request_queue->request_queue_.emplace_back(upgrade_lock_request);
    lock_request_queue->upgrading_ = txn->GetTransactionId();

    while (!IsGrantAllowed(upgrade_lock_request, lock_request_queue)) {
      lock_request_queue->cv_.wait(lock);
      // If the transaction was aborted in the meantime, do not grant the lock and return false
      if (txn->GetState() == TransactionState::ABORTED) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        lock_request_queue->request_queue_.remove(upgrade_lock_request);
        lock_request_queue->cv_.notify_all();
        return false;
      }
    }

    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    upgrade_lock_request->granted_ = true;
    InsertTableLockSet(txn, upgrade_lock_request);

    // maybe next request wait for this to be granted
    // if (lock_mode != LockMode::EXCLUSIVE) {
    //   lock_request_queue->cv_.notify_all();
    // }
    return true;
  }

  // first lock request for this txn
  std::shared_ptr<LockRequest> lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.emplace_back(lock_request);
  while (!IsGrantAllowed(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  lock_request->granted_ = true;
  InsertTableLockSet(txn, lock_request);

  // maybe next request wait for this to be granted
  // if (lock_mode != LockMode::EXCLUSIVE) {
  //   lock_request_queue->cv_.notify_all();
  // }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  // ensure that the transaction currently holds a lock on the resource it is attempting to unlock
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  // unlocking a table should only be allowed if the transaction does not hold locks on any row on that table
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  if (!(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set->at(oid).empty()) ||
      !(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set->at(oid).empty())) {
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  std::unique_lock lock(lock_request_queue->latch_);
  //  for (const auto& request : lock_request_queue->request_queue_) {
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); ++it) {
    auto request = *it;
    // ensure that the transaction currently holds a lock on the resource it is attempting to unlock
    if (request->txn_id_ != txn->GetTransactionId() || !request->granted_) {
      continue;
    }

    // Only unlocking S or X locks changes transaction state.
    if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
         (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE)) ||
        (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && request->lock_mode_ == LockMode::EXCLUSIVE) ||
        (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && request->lock_mode_ == LockMode::EXCLUSIVE)) {
      txn->SetState(TransactionState::SHRINKING);
    }

    // update the transaction's lock sets
    DeleteTableLockSet(txn, request);
    lock_request_queue->request_queue_.erase(it);
    lock_request_queue->cv_.notify_all();
    return true;
  }

  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // Row locking should not support Intention locks
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  // ISOLATION LEVEL
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn->GetState() == TransactionState::SHRINKING &&
          (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
          lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
  }

  // if an exclusive lock is attempted on a row, the transaction must hold either X, IX, or SIX on the table
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  std::unique_lock lock(lock_request_queue->latch_);
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
    auto request = *it;
    if (request->txn_id_ != txn->GetTransactionId()) {
      continue;
    }

    // it already has the lock
    if (request->lock_mode_ == lock_mode) {
      return true;
    }

    // Lock() should upgrade the lock held by the transaction
    // Multiple concurrent lock upgrades on the same resource
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }

    // can lock upgrade?
    if (!CanLockUpgrade(request->lock_mode_, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    // if possible, then drop current lock
    lock_request_queue->request_queue_.erase(it);
    DeleteRowLockSet(txn, request);

    auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    // find first location where not granted
    // auto itr = std::find_if(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
    //                         [](const std::shared_ptr<LockRequest> &request) { return !request->granted_; });
    // lock_request_queue->request_queue_.insert(itr, upgrade_lock_request);
    lock_request_queue->request_queue_.emplace_back(upgrade_lock_request);
    lock_request_queue->upgrading_ = txn->GetTransactionId();

    while (!IsGrantAllowed(upgrade_lock_request, lock_request_queue)) {
      lock_request_queue->cv_.wait(lock);
      // If the transaction was aborted in the meantime, do not grant the lock and return false
      if (txn->GetState() == TransactionState::ABORTED) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        lock_request_queue->request_queue_.remove(upgrade_lock_request);
        lock_request_queue->cv_.notify_all();
        return false;
      }
    }

    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    upgrade_lock_request->granted_ = true;
    InsertRowLockSet(txn, upgrade_lock_request);

    // maybe next request wait for this to be granted
    // if (lock_mode != LockMode::EXCLUSIVE) {
    //   lock_request_queue->cv_.notify_all();
    // }
    return true;
  }

  // first lock request for this txn
  std::shared_ptr<LockRequest> lock_request =
      std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.emplace_back(lock_request);
  while (!IsGrantAllowed(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  lock_request->granted_ = true;
  InsertRowLockSet(txn, lock_request);

  // maybe next request wait for this to be granted
  // if (lock_mode != LockMode::EXCLUSIVE) {
  //   lock_request_queue->cv_.notify_all();
  // }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  // ensure that the transaction currently holds a lock on the resource it is attempting to unlock
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();

  std::unique_lock lock(lock_request_queue->latch_);
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); ++it) {
    auto request = *it;
    // ensure that the transaction currently holds a lock on the resource it is attempting to unlock
    if (request->txn_id_ != txn->GetTransactionId() || !request->granted_) {
      continue;
    }

    // Only unlocking S or X locks changes transaction state.
    if (!force) {
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           request->lock_mode_ == LockMode::EXCLUSIVE)) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
    // update the transaction's lock sets
    DeleteRowLockSet(txn, request);
    lock_request_queue->request_queue_.erase(it);
    lock_request_queue->cv_.notify_all();
    return true;
  }
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2) == waits_for_[t1].end()) {
    waits_for_[t1].emplace_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto new_end = std::remove(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  waits_for_[t1].erase(new_end, waits_for_[t1].end());
}

auto LockManager::FindCycle(txn_id_t source_txn) -> bool {
  visited_.insert(source_txn);
  path_.emplace_back(source_txn);
  on_path_.insert(source_txn);
  for (auto next_txn : waits_for_[source_txn]) {
    if (visited_.find(next_txn) == visited_.end()) {
      bool found = FindCycle(next_txn);
      if (found) {
        return true;
      }
    }
    if (on_path_.find(next_txn) != on_path_.end()) {
      abort_txn_id_ = next_txn;
      return true;
    }
  }
  path_.pop_back();
  on_path_.erase(source_txn);
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  for (auto [id, _] : waits_for_) {
    if (visited_.find(id) != visited_.end()) {
      continue;
    }
    if (!FindCycle(id)) {
      path_.clear();
      on_path_.clear();
      continue;
    }
    auto it = path_.end() - 1;
    *txn_id = abort_txn_id_;
    while (*it != abort_txn_id_) {
      *txn_id = std::max(*txn_id, *it);
      --it;
    }
    path_.clear();
    on_path_.clear();
    visited_.clear();
    return true;
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &[t1, t2s] : waits_for_) {
    for (const auto &t2 : t2s) {
      edges.emplace_back(t1, t2);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      waits_for_.clear();
      {
        std::lock_guard lock(table_lock_map_latch_);
        for (const auto &[_, lock_request_queue] : table_lock_map_) {
          std::vector<txn_id_t> granted;
          std::vector<txn_id_t> waited;
          {
            std::lock_guard queue_lock(lock_request_queue->latch_);
            for (const auto &request : lock_request_queue->request_queue_) {
              auto txn = txn_manager_->GetTransaction(request->txn_id_);
              if (txn != nullptr && txn->GetState() != TransactionState::ABORTED) {
                if (request->granted_) {
                  granted.push_back(request->txn_id_);
                } else {
                  waited.push_back(request->txn_id_);
                }
              }
            }
          }
          for (const auto &u : granted) {
            for (const auto &v : waited) {
              AddEdge(u, v);
            }
          }
        }
      }

      {
        std::lock_guard lock(row_lock_map_latch_);
        for (const auto &[_, lock_request_queue] : row_lock_map_) {
          std::vector<txn_id_t> granted;
          std::vector<txn_id_t> waited;
          {
            std::lock_guard queue_lock(lock_request_queue->latch_);
            for (const auto &request : lock_request_queue->request_queue_) {
              auto txn = txn_manager_->GetTransaction(request->txn_id_);
              if (txn != nullptr && txn->GetState() != TransactionState::ABORTED) {
                if (request->granted_) {
                  granted.push_back(request->txn_id_);
                } else {
                  waited.push_back(request->txn_id_);
                }
              }
            }
          }
          for (const auto &u : granted) {
            for (const auto &v : waited) {
              AddEdge(u, v);
            }
          }
        }
      }

      while (true) {
        path_.clear();
        on_path_.clear();
        visited_.clear();
        txn_id_t txn_id;
        if (HasCycle(&txn_id)) {
          auto txn = txn_manager_->GetTransaction(txn_id);
          txn->SetState(TransactionState::ABORTED);
          // delete node
          waits_for_.erase(txn_id);
          for (auto [t1, _] : waits_for_) {
            RemoveEdge(t1, txn_id);
          }

          {
            bool found = false;
            std::lock_guard lock(table_lock_map_latch_);
            for (auto &[_, lock_request_queue] : table_lock_map_) {
              for (const auto &request : lock_request_queue->request_queue_) {
                if (request->txn_id_ == txn_id && !request->granted_) {
                  lock_request_queue->cv_.notify_all();  // already aborted
                  found = true;
                  break;
                }
              }
            }
            if (found) {
              continue;
            }
          }

          {
            std::lock_guard lock(row_lock_map_latch_);
            for (auto &[_, lock_request_queue] : row_lock_map_) {
              for (const auto &request : lock_request_queue->request_queue_) {
                if (request->txn_id_ == txn_id && !request->granted_) {
                  lock_request_queue->cv_.notify_all();
                  break;
                }
              }
            }
          }
        } else {
          break;
        }
      }
    }
  }
}

// according to compatibility table
auto LockManager::IsGrantAllowed(std::shared_ptr<LockRequest> &lock_request,
                                 const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  for (const auto &request : lock_request_queue->request_queue_) {
    if (!request->granted_) {
      break;
    }
    if (!AreLocksCompatible(lock_request->lock_mode_, request->lock_mode_)) {
      return false;
    }
  }

  if (lock_request_queue->upgrading_ == lock_request->txn_id_) {  // is upgrading
    return true;
  }
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {  // other txn
    return false;
  }

  // lock_request_queue->upgrading_ == INVALID_TXN_ID
  // new lock request for this table
  for (auto &request : lock_request_queue->request_queue_) {
    // not unlock
    if (request->txn_id_ == lock_request->txn_id_) {
      return true;
    }
    if (!request->granted_ && !AreLocksCompatible(lock_request->lock_mode_, request->lock_mode_)) {
      return false;
    }
  }
  return true;
}

void LockManager::InsertTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) {
  txn->LockTxn();
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
  }
  txn->UnlockTxn();
}

void LockManager::InsertRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) {
  txn->LockTxn();
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[lock_request->oid_].insert(lock_request->rid_);
      break;
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[lock_request->oid_].insert(lock_request->rid_);
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
  txn->UnlockTxn();
}

void LockManager::DeleteTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) {
  txn->LockTxn();
  switch (lock_request->lock_mode_) {
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
  }
  txn->UnlockTxn();
}

void LockManager::DeleteRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) {
  txn->LockTxn();
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[lock_request->oid_].erase(lock_request->rid_);
      break;
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[lock_request->oid_].erase(lock_request->rid_);
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
  txn->UnlockTxn();
}

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  switch (l1) {
    case LockMode::SHARED:
      if (l2 != LockMode::INTENTION_SHARED && l2 != LockMode::SHARED) {
        return false;
      }
      break;
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::INTENTION_SHARED:
      if (l2 == LockMode::EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (l2 != LockMode::INTENTION_SHARED && l2 != LockMode::INTENTION_EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (l2 != LockMode::INTENTION_SHARED) {
        return false;
      }
      break;
  }
  return true;
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  // IS -> [S, X, IX, SIX]
  // S -> [X, SIX]
  // IX -> [X, SIX]
  // SIX -> [X]
  switch (curr_lock_mode) {
    case LockMode::INTENTION_SHARED:
      if (requested_lock_mode == LockMode::SHARED || requested_lock_mode == LockMode::EXCLUSIVE ||
          requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
          requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        break;
      }
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      if (requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        break;
      }
    case LockMode::EXCLUSIVE:
      // do nothing
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (requested_lock_mode == LockMode::EXCLUSIVE) {
        break;
      }
    default:
      return false;
  }
  return true;
}

}  // namespace bustub
