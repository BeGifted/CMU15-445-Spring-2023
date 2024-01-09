//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IndexOf(const KeyType &key, const KeyComparator &comparator) const -> int {
  auto cmp = [&comparator](const MappingType &lhs, const MappingType &rhs) -> bool {
    return comparator(lhs.first, rhs.first) < 0;
  };
  int index = std::upper_bound(array_ + 1, array_ + GetSize(), MappingType(key, ValueType{}), cmp) - array_;
  return index;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueOn(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  int index = IndexOf(key, comparator);
  return array_[index - 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ResetIndex(const KeyType &old_key, const KeyType &new_key,
                                                const KeyComparator &comparator) {
  int index = IndexOf(old_key, comparator) - 1;
  array_[index].first = new_key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(BufferPoolManager *bpm, KeyType &new_key, page_id_t &r_page_id) {
  BasicPageGuard guard = bpm->NewPageGuarded(&r_page_id);
  auto r_page = guard.AsMut<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
  r_page->Init(GetMaxSize());
  std::copy(array_ + GetMinSize(), array_ + GetMaxSize() + 1, r_page->array_);
  r_page->SetSize(GetMaxSize() - GetMinSize() + 1);
  SetSize(GetMinSize());
  new_key = r_page->KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &l_v, const ValueType &r_v,
                                            const KeyComparator &comparator) {
  std::cout << "Internal Insert size: " << GetSize() << " key: " << key << std::endl;
  if (GetSize() == 0) {
    array_[0] = MappingType(key, l_v);
    array_[1] = MappingType(key, r_v);
    IncreaseSize(2);
  } else {
    int index = IndexOf(key, comparator);
    if (index != GetSize()) {
      std::copy_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
      //      std::copy(array_ + index, array_ + GetSize(), array_ + index + 1);
    }
    array_[index] = MappingType(key, r_v);
    IncreaseSize(1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitPrev(BufferPoolManager *bpm, page_id_t page_id, KeyType &old_key,
                                               KeyType &new_key) -> bool {
  WritePageGuard guard = bpm->FetchPageWrite(page_id);
  auto page = guard.AsMut<BPlusTreeInternalPage>();
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitNext(BufferPoolManager *bpm, page_id_t page_id, KeyType &old_key,
                                               KeyType &new_key) -> bool {
  WritePageGuard guard = bpm->FetchPageWrite(page_id);
  auto page = guard.AsMut<BPlusTreeInternalPage>();
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

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
