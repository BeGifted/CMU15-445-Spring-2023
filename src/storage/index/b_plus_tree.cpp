#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard guard_page = bpm_->FetchPageRead(root_page->root_page_id_);
  guard.Drop();
  while (true) {
    // B_PLUS_TREE_INTERNAL_PAGE_TYPE => ValueType = rid
    auto page = guard_page.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (page->IsLeafPage()) {
      auto leaf_page = guard_page.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      std::optional<ValueType> value = leaf_page->ValueOn(key, comparator_);
      if (value.has_value()) {  // std::optional has a value
        result->emplace_back(value.value());
        return true;
      }
      return false;
    }
    guard_page = bpm_->FetchPageRead(page->ValueOn(key, comparator_));
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  ctx.header_page_.emplace(bpm_->FetchPageWrite(header_page_id_));
  auto root_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();

  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page;
  //  std::cout << "Insert key: " << key << " RID: " << value;
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {  // empty
    page_id_t page_id;
    BasicPageGuard guard_page = bpm_->NewPageGuarded(&page_id);
    root_page->root_page_id_ = page_id;
    leaf_page = guard_page.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    leaf_page->Init(leaf_max_size_);
  } else {
    ctx.write_set_.emplace_back(bpm_->FetchPageWrite(root_page->root_page_id_));
    ctx.prev_.emplace_back(INVALID_PAGE_ID);
    ctx.next_.emplace_back(INVALID_PAGE_ID);

    // find leaf page
    while (true) {
      auto internal_page = ctx.write_set_.back().As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      //      std::cout << "isLeaf: " << internal_page->IsLeafPage() << " internal size: " << internal_page->GetSize()
      //                << " maxsize: " << internal_page->GetMaxSize() << std::endl;
      if (internal_page->GetSize() < internal_page->GetMaxSize()) {
        while (ctx.write_set_.size() > 1) {
          //          std::cout << "pop" << std::endl;
          if (ctx.header_page_.has_value()) {  // drop the head page guard
            ctx.header_page_ = std::nullopt;
          }
          ctx.write_set_.pop_front();
          ctx.prev_.pop_front();
          ctx.next_.pop_front();
        }
      }

      if (internal_page->IsLeafPage()) {
        break;
      }

      // down
      int index = internal_page->IndexOf(key, comparator_) - 1;  // upper_bound
      ctx.write_set_.emplace_back(bpm_->FetchPageWrite(internal_page->ValueAt(index)));
      ctx.prev_.emplace_back(index == 0 ? INVALID_PAGE_ID : internal_page->ValueAt(index - 1));
      ctx.next_.emplace_back(index == internal_page->GetSize() - 1 ? INVALID_PAGE_ID
                                                                   : internal_page->ValueAt(index + 1));
    }
    leaf_page = ctx.write_set_.back().AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  }

  // insert in leaf page
  if (!leaf_page->Insert(key, value, comparator_)) {
    return false;  // already have
  }

  // size satisfy
  if (leaf_page->GetSize() <= leaf_page->GetMaxSize()) {
    //    Print(bpm_);
    //    std::cout << DrawBPlusTree() << std::endl;
    return true;
  }

  // split needed
  // split first k/v to prev(left)
  //  std::cout << "Start Split" << std::endl;
  if (ctx.prev_.back() != INVALID_PAGE_ID) {
    KeyType old_kay{};
    KeyType new_kay{};
    if (leaf_page->SplitPrev(bpm_, ctx.prev_.back(), old_kay, new_kay)) {
      //      std::cout << "Split to Prev old_key: " << old_kay << " new_key: " << new_kay << std::endl;
      ctx.write_set_.pop_back();
      auto internal_page = ctx.write_set_.back().AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      internal_page->ResetIndex(old_kay, new_kay, comparator_);
      return true;
    }
  }
  // split last k/v to next(right)
  if (ctx.next_.back() != INVALID_PAGE_ID) {
    KeyType old_kay{};
    KeyType new_kay{};
    if (leaf_page->SplitNext(bpm_, ctx.next_.back(), old_kay, new_kay)) {
      //      std::cout << "Split to Next old_key: " << old_kay << " new_key: " << new_kay << std::endl;
      ctx.write_set_.pop_back();
      auto internal_page = ctx.write_set_.back().AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      internal_page->ResetIndex(old_kay, new_kay, comparator_);
      return true;
    }
  }

  // cannot split to prev & nex
  auto l_page_id = ctx.write_set_.back().PageId();
  KeyType new_key;
  page_id_t r_page_id;
  leaf_page->Split(bpm_, new_key, r_page_id);
  //  std::cout << "Split Leaf to Half new_key: " << new_key << " write_set_.size(): " << ctx.write_set_.size()
  //            << std::endl;
  ctx.write_set_.pop_back();  // pop leaf
  ctx.prev_.pop_back();
  ctx.next_.pop_back();

  while (!ctx.write_set_.empty()) {
    //    std::cout << ctx.write_set_.size() << std::endl;
    auto internal_page = ctx.write_set_.back().AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    internal_page->Insert(new_key, l_page_id, r_page_id, comparator_);

    // size satisfy
    if (internal_page->GetSize() <= internal_page->GetMaxSize()) {
      return true;
    }

    // split needed
    // split first k/v to prev(left)
    if (ctx.prev_.back() != INVALID_PAGE_ID) {
      KeyType old_kay{};
      KeyType new_kay{};
      if (internal_page->SplitPrev(bpm_, ctx.prev_.back(), old_kay, new_kay)) {
        ctx.write_set_.pop_back();
        auto parent_internal_page =
            ctx.write_set_.back().AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        parent_internal_page->ResetIndex(old_kay, new_kay, comparator_);
        return true;
      }
    }
    // split last k/v to next(right)
    if (ctx.next_.back() != INVALID_PAGE_ID) {
      KeyType old_kay{};
      KeyType new_kay{};
      if (internal_page->SplitNext(bpm_, ctx.next_.back(), old_kay, new_kay)) {
        ctx.write_set_.pop_back();
        auto parent_internal_page =
            ctx.write_set_.back().AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        parent_internal_page->ResetIndex(old_kay, new_kay, comparator_);
        return true;
      }
    }

    // cannot split to prev & next
    l_page_id = ctx.write_set_.back().PageId();
    internal_page->Split(bpm_, new_key, r_page_id);

    ctx.write_set_.pop_back();
    ctx.prev_.pop_back();
    ctx.next_.pop_back();
  }

  root_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  page_id_t page_id;
  BasicPageGuard guard = bpm_->NewPageGuarded(&page_id);
  root_page->root_page_id_ = page_id;
  auto internal_page = guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  internal_page->Init(internal_max_size_);
  internal_page->Insert(new_key, l_page_id, r_page_id, comparator_);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  ctx.header_page_.emplace(bpm_->FetchPageWrite(header_page_id_));
  auto root_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();

  //  std::cout << "Remove key: " << key << std::endl;
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {  // empty
    return;
  }

  ctx.write_set_.emplace_back(bpm_->FetchPageWrite(root_page->root_page_id_));
  ctx.prev_.emplace_back(INVALID_PAGE_ID);
  ctx.next_.emplace_back(INVALID_PAGE_ID);

  while (true) {
    auto internal_page = ctx.write_set_.back().AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (internal_page->GetSize() > internal_page->GetMinSize()) {
      while (ctx.write_set_.size() > 1) {
        if (ctx.header_page_.has_value()) {
          ctx.header_page_ = std::nullopt;
        }
        ctx.write_set_.pop_front();
        ctx.prev_.pop_front();
        ctx.next_.pop_front();
      }
    }

    if (internal_page->IsLeafPage()) {
      break;
    }

    int index = internal_page->IndexOf(key, comparator_) - 1;
    ctx.write_set_.emplace_back(bpm_->FetchPageWrite(internal_page->ValueAt(index)));
    ctx.prev_.emplace_back(index == 0 ? INVALID_PAGE_ID : internal_page->ValueAt(index - 1));
    ctx.next_.emplace_back(index == internal_page->GetSize() - 1 ? INVALID_PAGE_ID : internal_page->ValueAt(index + 1));
  }

  auto leaf_page = ctx.write_set_.back().AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();

  if (!leaf_page->Delete(key, comparator_)) {
    return;  // key not find
  }

  // size satisfy after delete
  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    return;
  }

  // split needed
  // split first k/v to prev(left)
  //  std::cout << "Start Merge" << std::endl;
  if (ctx.prev_.back() != INVALID_PAGE_ID) {
    KeyType old_kay{};
    KeyType new_kay{};
    if (leaf_page->MergePrev(bpm_, ctx.prev_.back(), old_kay, new_kay)) {
      //      std::cout << "Merge From Prev old_key: " << old_kay << " new_key: " << new_kay << std::endl;
      ctx.write_set_.pop_back();
      auto internal_page = ctx.write_set_.back().AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      internal_page->ResetIndex(old_kay, new_kay, comparator_);
      return;
    }
  }
  // split last k/v to next(right)
  if (ctx.next_.back() != INVALID_PAGE_ID) {
    KeyType old_kay{};
    KeyType new_kay{};
    if (leaf_page->MergeNext(bpm_, ctx.next_.back(), old_kay, new_kay)) {
      //      std::cout << "Merge from Next old_key: " << old_kay << " new_key: " << new_kay << std::endl;
      ctx.write_set_.pop_back();
      auto internal_page = ctx.write_set_.back().AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      internal_page->ResetIndex(old_kay, new_kay, comparator_);
      return;
    }
  }

  KeyType delete_key = leaf_page->Merge(bpm_, ctx.prev_.back(), ctx.next_.back());
  ctx.write_set_.pop_back();
  ctx.prev_.pop_back();
  ctx.next_.pop_back();

  while (!ctx.write_set_.empty()) {
    auto internal_page = ctx.write_set_.back().AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    internal_page->Delete(delete_key, comparator_);

    // size satisfy
    if (internal_page->GetSize() >= internal_page->GetMinSize()) {
      return;
    }

    if (ctx.prev_.back() != INVALID_PAGE_ID) {
      KeyType old_kay{};
      KeyType new_kay{};
      if (internal_page->MergePrev(bpm_, ctx.prev_.back(), old_kay, new_kay)) {
        ctx.write_set_.pop_back();
        auto page = ctx.write_set_.back().AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        page->ResetIndex(old_kay, new_kay, comparator_);
        return;
      }
    }

    if (ctx.next_.back() != INVALID_PAGE_ID) {
      KeyType old_kay{};
      KeyType new_kay{};
      if (internal_page->MergeNext(bpm_, ctx.next_.back(), old_kay, new_kay)) {
        ctx.write_set_.pop_back();
        auto page = ctx.write_set_.back().AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        page->ResetIndex(old_kay, new_kay, comparator_);
        return;
      }
    }

    delete_key = internal_page->Merge(bpm_, ctx.prev_.back(), ctx.next_.back());
    ctx.write_set_.pop_back();
    ctx.prev_.pop_back();
    ctx.next_.pop_back();
  }

  root_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  ReadPageGuard root_guard = bpm_->FetchPageRead(root_page->root_page_id_);
  auto internal_page = root_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  if (internal_page->GetSize() != 0 && internal_page->IsLeafPage()) {
    return;
  }
  if (internal_page->GetSize() == 0) {  // is leaf
    root_page->root_page_id_ = INVALID_PAGE_ID;
  }
  if (internal_page->GetSize() == 1) {
    root_page->root_page_id_ = internal_page->ValueAt(0);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(bpm_);
  }
  page_id_t page_id = root_page->root_page_id_;
  int index = 0;
  ReadPageGuard read_guard = bpm_->FetchPageRead(page_id);
  guard.Drop();
  while (true) {
    auto internal_page = read_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (internal_page->IsLeafPage()) {
      return INDEXITERATOR_TYPE(bpm_, page_id, index);
    }
    page_id = internal_page->ValueAt(0);
    read_guard = bpm_->FetchPageRead(page_id);
  }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(bpm_);
  }
  page_id_t page_id = root_page->root_page_id_;
  int index = 0;
  ReadPageGuard read_guard = bpm_->FetchPageRead(page_id);
  guard.Drop();
  while (true) {
    auto internal_page = read_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (internal_page->IsLeafPage()) {
      index = read_guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>()->IndexOf(key, comparator_);
      return INDEXITERATOR_TYPE(bpm_, page_id, index);
    }
    page_id = internal_page->ValueOn(key, comparator_);
    read_guard = bpm_->FetchPageRead(page_id);
  }
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
