//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <iostream>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock scoped_latch(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  size_t time_gap = 0;
  size_t timestamp = SIZE_MAX;
  frame_id_t id = INT32_MIN;
  for (auto &kv : node_store_) {
    if (kv.second.is_evictable_) {
      if (kv.second.history_.size() < k_) {
        time_gap = SIZE_MAX;
        if (kv.second.history_.back() < timestamp) {
          timestamp = kv.second.history_.back();
          id = kv.first;
        }
      } else {
        // auto it = kv.second.history_.begin();
        // std::advance(it, k_ - 1);
        if (current_timestamp_ - kv.second.history_.back() > time_gap) {
          time_gap = current_timestamp_ - kv.second.history_.back();
          id = kv.first;
        }
      }
    }
  }

  if (id == INT32_MIN) {
    return false;
  }
  node_store_[id].history_.clear();
  node_store_[id].is_evictable_ = false;
  *frame_id = id;
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::scoped_lock scoped_latch(latch_);
  current_timestamp_++;
  if (node_store_.find(frame_id) != node_store_.end()) {
    node_store_[frame_id].history_.push_front(current_timestamp_);
    if (node_store_[frame_id].history_.size() > k_) {
      node_store_[frame_id].history_.pop_back();
    }
  } else {
    node_store_[frame_id] = LRUKNode{{current_timestamp_}, k_, frame_id, false};
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // RecordAccess(frame_id);
  if (frame_id > static_cast<int32_t>(replacer_size_)) {
    throw ExecutionException("frame id is not invalid.");
  }
  latch_.lock();
  if (node_store_.find(frame_id) != node_store_.end()) {
    if (node_store_[frame_id].is_evictable_ && !set_evictable) {
      node_store_[frame_id].is_evictable_ = set_evictable;
      curr_size_--;
    } else if (!node_store_[frame_id].is_evictable_ && set_evictable) {
      node_store_[frame_id].is_evictable_ = set_evictable;
      curr_size_++;
      // if (curr_size_ > replacer_size_) {
      //   frame_id_t ret_id;
      //   latch_.unlock();
      //   Evict(&ret_id);
      //   return;
      // }
    }
  } else {
    latch_.unlock();
    throw ExecutionException("frame is not in node_store_.");
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock scoped_latch(latch_);
  if (node_store_.find(frame_id) != node_store_.end()) {
    if (!node_store_[frame_id].is_evictable_) {
      latch_.unlock();
      throw ExecutionException("frame is not evictable.");
    }
    node_store_.erase(frame_id);
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock scoped_latch(latch_);
  return curr_size_;
}

}  // namespace bustub
