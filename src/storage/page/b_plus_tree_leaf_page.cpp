//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IndexOf(const KeyType &key, const KeyComparator &comparator) const -> int {
  auto cmp = [&comparator](const MappingType &lhs, const MappingType &rhs) -> bool {
    return comparator(lhs.first, rhs.first) < 0;
  };
  // lower_bound to find same key
  int index = std::lower_bound(array_, array_ + GetSize(), MappingType(key, ValueType{}), cmp) - array_;
  return index;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueOn(const KeyType &key, const KeyComparator &comparator) const
    -> std::optional<ValueType> {
  int index = IndexOf(key, comparator);
  KeyType key_find = KeyAt(index);
  if (index == GetSize() || comparator(key_find, key) != 0) {
    return std::nullopt;
  }
  //  std::cout << "ValueOn " << key << " is " << array_[index].second;
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  int index = IndexOf(key, comparator);
  KeyType key_find = KeyAt(index);
  if (index != GetSize() && comparator(key_find, key) == 0) {  // already have
    return false;
  }
  //  for (int i = 0; i < GetSize(); i++) {
  //    std::cout << array_[i].first << " ";
  //  }
  //  std::cout << std::endl;
  std::copy_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);

  //  std::copy(array_ + index, array_ + GetSize(), array_ + index + 1);
  array_[index] = MappingType(key, value);
  IncreaseSize(1);
  //  for (int i = 0; i < GetSize(); i++) {
  //    std::cout << array_[i].first << " ";
  //  }
  //  std::cout << std::endl;
  //  std::cout << "Leaf Insert size: " << GetSize() << " key: " << key << std::endl;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SplitPrev(BufferPoolManager *bpm, page_id_t page_id, KeyType &old_key,
                                           KeyType &new_key) -> bool {
  WritePageGuard guard = bpm->FetchPageWrite(page_id);
  auto page = guard.AsMut<BPlusTreeLeafPage>();
  if (page->GetSize() >= page->GetMaxSize()) {
    return false;
  }
  old_key = KeyAt(0);
  page->array_[page->GetSize()] = array_[0];
  std::copy(array_ + 1, array_ + GetSize(), array_);  // forward
  page->IncreaseSize(1);
  IncreaseSize(-1);
  new_key = KeyAt(0);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SplitNext(BufferPoolManager *bpm, page_id_t page_id, KeyType &old_key,
                                           KeyType &new_key) -> bool {
  WritePageGuard guard = bpm->FetchPageWrite(page_id);
  auto page = guard.AsMut<BPlusTreeLeafPage>();
  if (page->GetSize() >= page->GetMaxSize()) {
    return false;
  }
  old_key = page->KeyAt(0);
  std::copy_backward(page->array_, page->array_ + page->GetSize(), page->array_ + page->GetSize() + 1);  // backward
  //  std::copy(page->array_, page->array_ + page->GetSize(), page->array_ + 1);  // backward
  page->array_[0] = array_[GetSize() - 1];
  page->IncreaseSize(1);
  IncreaseSize(-1);
  new_key = page->KeyAt(0);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Split(BufferPoolManager *bpm, KeyType &new_key, page_id_t &r_page_id) {
  bpm->NewPage(&r_page_id);
  WritePageGuard guard = bpm->FetchPageWrite(r_page_id);
  bpm->UnpinPage(r_page_id, true);
  auto r_page = guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  r_page->Init(GetMaxSize());
  r_page->SetNextPageId(GetNextPageId());
  SetNextPageId(r_page_id);
  std::copy(array_ + GetMinSize(), array_ + GetMaxSize() + 1, r_page->array_);
  r_page->SetSize(GetMaxSize() - GetMinSize() + 1);
  SetSize(GetMinSize());
  new_key = r_page->KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &comparator) -> bool {
  int index = IndexOf(key, comparator);
  KeyType key_find = array_[index].first;
  if (index == GetSize() || comparator(key_find, key) != 0) {
    return false;
  }
  std::copy(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MergePrev(BufferPoolManager *bpm, page_id_t page_id, KeyType &old_key,
                                           KeyType &new_key) -> bool {
  WritePageGuard guard = bpm->FetchPageWrite(page_id);
  auto page = guard.AsMut<BPlusTreeLeafPage>();
  if (page->GetSize() <= page->GetMinSize()) {
    return false;
  }
  old_key = KeyAt(0);
  std::copy_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  array_[0] = page->array_[page->GetSize() - 1];
  page->IncreaseSize(-1);
  IncreaseSize(1);
  new_key = KeyAt(0);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MergeNext(BufferPoolManager *bpm, page_id_t page_id, KeyType &old_key,
                                           KeyType &new_key) -> bool {
  WritePageGuard guard = bpm->FetchPageWrite(page_id);
  auto page = guard.AsMut<BPlusTreeLeafPage>();
  if (page->GetSize() <= page->GetMinSize()) {
    return false;
  }
  old_key = page->KeyAt(0);
  array_[GetSize()] = page->array_[0];
  std::copy(page->array_ + 1, page->array_ + page->GetSize(), page->array_);
  page->IncreaseSize(-1);
  IncreaseSize(1);
  new_key = page->KeyAt(0);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(BufferPoolManager *bpm, page_id_t l_page_id, page_id_t r_page_id) -> KeyType {
  if (l_page_id != INVALID_PAGE_ID) {
    WritePageGuard guard = bpm->FetchPageWrite(l_page_id);
    auto page = guard.AsMut<BPlusTreeLeafPage>();
    std::copy(array_, array_ + GetSize(), page->array_ + page->GetSize());
    page->IncreaseSize(GetSize());
    page->SetNextPageId(GetNextPageId());
    SetSize(0);
    return KeyAt(0);
  }
  if (r_page_id != INVALID_PAGE_ID) {
    WritePageGuard guard = bpm->FetchPageWrite(r_page_id);
    auto page = guard.AsMut<BPlusTreeLeafPage>();
    std::copy(page->array_, page->array_ + page->GetSize(), array_ + GetSize());
    IncreaseSize(page->GetSize());
    SetNextPageId(page->GetNextPageId());
    page->SetSize(0);
    return page->KeyAt(0);
  }
  return KeyAt(0);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
