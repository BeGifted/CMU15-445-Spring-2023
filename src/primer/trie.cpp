#include "primer/trie.h"
#include <iostream>
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  std::shared_ptr<const TrieNode> cur = root_;
  for (auto ch : key) {
    if (!cur) {
      return nullptr;
    }
    if (cur->children_.find(ch) != cur->children_.end()) {
      cur = cur->children_.at(ch);
    } else {
      return nullptr;
    }
  }
  const auto *res = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  if (res) {
    return res->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  std::stack<std::shared_ptr<const TrieNode>> st;
  std::shared_ptr<const TrieNode> cur = root_;
  std::size_t len = key.length();
  std::size_t idx = 0;

  while (idx < len && cur) {
    char ch = key[idx++];
    st.push(cur);
    if (cur->children_.find(ch) != cur->children_.end()) {
      cur = cur->children_.at(ch);
    } else {
      cur = nullptr;
    }
  }

  std::shared_ptr<const TrieNodeWithValue<T>> leaf =
      cur ? std::make_shared<const TrieNodeWithValue<T>>(cur->children_, std::make_shared<T>(std::move(value)))
          : std::make_shared<const TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));

  std::shared_ptr<const TrieNode> child = leaf;
  cur = child;

  while (idx < len) {
    char ch = key[--len];
    cur = std::make_shared<const TrieNode>(std::map<char, std::shared_ptr<const TrieNode>>{{ch, child}});
    child = cur;
  }

  std::size_t siz = st.size();
  for (size_t i = siz - 1; i < siz; i--) {
    char ch = key[i];
    cur = std::shared_ptr<const TrieNode>(st.top()->Clone());
    st.pop();
    const_cast<TrieNode *>(cur.get())->children_[ch] = child;
    child = cur;
  }
  return Trie(cur);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  std::stack<std::shared_ptr<const TrieNode>> st;
  std::shared_ptr<const TrieNode> cur = root_;
  std::size_t len = key.length();
  std::size_t idx = 0;

  while (idx < len && cur) {
    char ch = key[idx++];
    st.push(cur);
    if (cur->children_.find(ch) != cur->children_.end()) {
      cur = cur->children_.at(ch);
    } else {
      cur = nullptr;
    }
  }

  if (idx != len || !cur || !cur->is_value_node_) {  // not find
    return *this;
  }

  std::shared_ptr<const TrieNode> leaf =
      cur->children_.empty() ? nullptr : std::make_shared<const TrieNode>(cur->children_);

  std::shared_ptr<const TrieNode> child = leaf;
  cur = leaf;

  std::size_t siz = st.size();
  for (size_t i = siz - 1; i < siz; i--) {
    char ch = key[i];
    cur = std::shared_ptr<const TrieNode>(st.top()->Clone());
    st.pop();
    const_cast<TrieNode *>(cur.get())->children_[ch] = child;
    child = cur;
  }
  return Trie(cur);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
