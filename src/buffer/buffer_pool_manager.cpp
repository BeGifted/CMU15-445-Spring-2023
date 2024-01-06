//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock scoped_latch(latch_);
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    bool ret = replacer_->Evict(&frame_id);
    if (!ret) {
      return nullptr;
    }
  }
  *page_id = AllocatePage();
  Page &pages = pages_[frame_id];
  if (pages.IsDirty()) {
    disk_manager_->WritePage(pages.GetPageId(), pages.GetData());
  }
  page_table_.erase(pages.GetPageId());
  page_table_[*page_id] = frame_id;
  pages.ResetMemory();
  pages.page_id_ = *page_id;
  pages.pin_count_ = 1;
  pages.is_dirty_ = false;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  std::scoped_lock scoped_latch(latch_);
  frame_id_t frame_id;
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
  } else {
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      bool ret = replacer_->Evict(&frame_id);
      if (!ret) {
        return nullptr;
      }
    }
    Page &pages = pages_[frame_id];
    if (pages.IsDirty()) {
      disk_manager_->WritePage(pages.GetPageId(), pages.GetData());
    }
    page_table_.erase(pages.page_id_);
    page_table_[page_id] = frame_id;
    pages.ResetMemory();
    pages.page_id_ = page_id;
    pages.pin_count_ = 1;
    pages.is_dirty_ = false;

    disk_manager_->ReadPage(page_id, pages.GetData());
  }
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  std::scoped_lock scoped_latch(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page &pages = pages_[frame_id];
  if (pages.GetPinCount() <= 0) {
    return false;
  }
  pages.pin_count_--;
  if (pages.GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  if (is_dirty) {
    pages.is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock scoped_latch(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page &pages = pages_[frame_id];
  disk_manager_->WritePage(page_id, pages.GetData());
  pages.is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock scoped_latch(latch_);
  for (auto &[page_id, frame_id] : page_table_) {
    Page &pages = pages_[frame_id];
    disk_manager_->WritePage(page_id, pages.GetData());
    pages.is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock scoped_latch(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page &pages = pages_[frame_id];
  if (pages.GetPinCount() > 0) {
    return false;
  }
  if (pages.IsDirty()) {
    disk_manager_->WritePage(pages.GetPageId(), pages.GetData());
  }
  free_list_.push_back(frame_id);
  pages.ResetMemory();
  DeallocatePage(page_id);
  page_table_.erase(page_id);
  try {
    replacer_->Remove(frame_id);
  } catch (const ExecutionException &e) {
    return false;
  }
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
