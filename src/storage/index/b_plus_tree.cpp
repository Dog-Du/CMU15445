#include "storage/index/b_plus_tree.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "include/buffer/buffer_pool_manager_instance.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      begin_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      leaf_min_size_((leaf_max_size) >> 1),
      internal_max_size_(internal_max_size),
      internal_min_size_((1 + internal_max_size) >> 1) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LowerBound(InternalMappingType *first, InternalMappingType *last, const KeyType &key)
    -> InternalMappingType * {
  int len = last - first;

  while (len > 0) {
    int half = len >> 1;
    auto *middle = first + half;

    if (comparator_(middle->first, key) == -1) {
      first = middle;
      ++first;
      len = len - half - 1;
    } else {
      len = half;
    }
  }

  return first;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LowerBound(LeafMappingType *first, LeafMappingType *last, const KeyType &key)
    -> LeafMappingType * {
  int len = last - first;

  while (len > 0) {
    int half = len >> 1;
    auto *middle = first + half;

    if (comparator_(middle->first, key) == -1) {
      first = middle;
      ++first;
      len = len - half - 1;
    } else {
      len = half;
    }
  }

  return first;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UpperBound(LeafMappingType *first, LeafMappingType *last, const KeyType &key)
    -> LeafMappingType * {
  int len = last - first;

  while (len > 0) {
    int half = len >> 1;
    auto *middle = first + half;

    if (comparator_(middle->first, key) == 1) {
      len = half;
    } else {
      first = middle;
      ++first;
      len = len - half - 1;
    }
  }

  return first;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UpperBound(InternalMappingType *first, InternalMappingType *last, const KeyType &key)
    -> InternalMappingType * {
  int len = last - first;

  while (len > 0) {
    int half = len >> 1;
    auto *middle = first + half;

    if (comparator_(middle->first, key) == 1) {
      len = half;
    } else {
      first = middle;
      ++first;
      len = len - half - 1;
    }
  }

  return first;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchInternalPage(page_id_t page_id) -> InternalPage * {
  return reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewInternalPage(page_id_t parent_id) -> InternalPage * {
  page_id_t new_page_id = -1;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  auto *internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
  internal_page->Init(new_page_id, parent_id, internal_max_size_);
  return internal_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchLeafPage(page_id_t page_id) -> LeafPage * {
  return reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewLeafPage(page_id_t parent_id) -> LeafPage * {
  page_id_t new_page_id = -1;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  auto *internal_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  internal_page->Init(new_page_id, parent_id, leaf_max_size_);
  return internal_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *old_leaf_page) -> bool {
  if (old_leaf_page->GetSize() < leaf_max_size_) {
    return true;
  }

  LeafPage *new_leaf_page = NewLeafPage(old_leaf_page->GetParentPageId());

  // 新叶子节点应该指向原来叶子结点指向的，因为我总是向右分裂。
  new_leaf_page->SetNextPageId(old_leaf_page->GetNextPageId());
  old_leaf_page->SetNextPageId(new_leaf_page->GetPageId());
  old_leaf_page->SetSize(leaf_min_size_);
  new_leaf_page->SetSize(leaf_max_size_ - leaf_min_size_);

  auto *new_array = new_leaf_page->GetArray();
  auto *old_array = old_leaf_page->GetArray();

  for (int i = leaf_min_size_, j = 0; i < leaf_max_size_; ++i, ++j) {
    new_array[j] = std::move(old_array[i]);
  }

  // 根分裂需要再申请一个节点。
  if (old_leaf_page->IsRootPage()) {
    InternalPage *new_root_page = NewInternalPage(INVALID_PAGE_ID);
    root_page_id_ = new_root_page->GetPageId();

    old_leaf_page->SetParentPageId(root_page_id_);
    new_leaf_page->SetParentPageId(root_page_id_);
    new_root_page->SetSize(2);

    auto *root_array = new_root_page->GetArray();
    root_array[1].first = new_array[0].first;
    root_array[0].second = old_leaf_page->GetPageId();
    root_array[1].second = new_leaf_page->GetPageId();

    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
    return true;
  }

  InternalPage *parent = FetchInternalPage(old_leaf_page->GetParentPageId());
  auto *parent_array = parent->GetArray();

  // 找插入位置不减1，找向下的位置要减一。
  int i = LowerBound(parent_array + 1, parent_array + parent->GetSize(), new_array[0].first) - parent_array;

  for (int j = parent->GetSize(); j > i; --j) {
    parent_array[j] = std::move(parent_array[j - 1]);
  }

  parent_array[i].first = new_array[0].first;
  parent_array[i].second = new_leaf_page->GetPageId();

  parent->IncreaseSize(1);

  bool ret = parent->GetSize() <= internal_max_size_;

  buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *old_internal_page) -> bool {
  if (old_internal_page->GetSize() <= internal_max_size_) {
    return true;
  }

  InternalPage *new_internal_page = NewInternalPage(old_internal_page->GetParentPageId());

  new_internal_page->SetSize(internal_max_size_ - internal_min_size_ + 1);
  old_internal_page->SetSize(internal_min_size_);

  auto *new_array = new_internal_page->GetArray();
  auto *old_array = old_internal_page->GetArray();

  for (int i = internal_min_size_ + 1, j = 1; i <= internal_max_size_; ++i, ++j) {
    new_array[j] = std::move(old_array[i]);

    // 改变转向。
    InternalPage *tmp_page = FetchInternalPage(new_array[j].second);
    tmp_page->SetParentPageId(new_internal_page->GetPageId());
    buffer_pool_manager_->UnpinPage(new_array[j].second, true);
  }

  // 处理第0个儿子。
  // 把internal_min_size_的first给父亲，把second给新节点。
  {
    new_array[0].second = old_array[internal_min_size_].second;
    InternalPage *tmp_page = FetchInternalPage(new_array[0].second);
    tmp_page->SetParentPageId(new_internal_page->GetPageId());
    buffer_pool_manager_->UnpinPage(new_array[0].second, true);
  }

  if (old_internal_page->IsRootPage()) {
    InternalPage *new_root_page = NewInternalPage(INVALID_PAGE_ID);
    root_page_id_ = new_root_page->GetPageId();

    new_root_page->SetSize(2);
    old_internal_page->SetParentPageId(root_page_id_);
    new_internal_page->SetParentPageId(root_page_id_);

    auto *root_array = new_root_page->GetArray();
    // 草你妈的，这里应该是old_array，打错了。
    root_array[1].first = old_array[internal_min_size_].first;
    root_array[0].second = old_internal_page->GetPageId();
    root_array[1].second = new_internal_page->GetPageId();

    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->UnpinPage(new_internal_page->GetPageId(), true);
    return true;
  }

  InternalPage *parent = FetchInternalPage(old_internal_page->GetParentPageId());

  auto *parent_array = parent->GetArray();
  int i = LowerBound(parent_array + 1, parent_array + parent->GetSize(), old_array[internal_min_size_ + 1].first) -
          parent_array;

  for (int j = parent->GetSize(); j > i; --j) {
    parent_array[j] = std::move(parent_array[j - 1]);
  }

  parent_array[i].first = old_array[internal_min_size_].first;
  parent_array[i].second = new_internal_page->GetPageId();
  parent->IncreaseSize(1);

  bool ret = parent->GetSize() <= internal_max_size_;

  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_internal_page->GetPageId(), true);

  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeInternal(InternalPage *&old_internal_page) -> bool {
  if (old_internal_page->GetSize() >= internal_min_size_ || old_internal_page->IsRootPage()) {
    return true;
  }

  InternalPage *parent = FetchInternalPage(old_internal_page->GetParentPageId());
  auto *parent_array = parent->GetArray();
  auto *old_array = old_internal_page->GetArray();

  int i = UpperBound(parent_array + 1, parent_array + parent->GetSize(), old_array[1].first) - parent_array - 1;

  // 借孩子：
  if (i - 1 >= 0) {
    InternalPage *left_node = FetchInternalPage(parent->ValueAt(i - 1));

    if (left_node->GetSize() > internal_min_size_) {
      auto *left_array = left_node->GetArray();
      // 把0也迁徙过去。
      for (int j = old_internal_page->GetSize(); j > 0; --j) {
        old_array[j] = std::move(old_array[j - 1]);
      }
      old_array[1].first = parent_array[i].first;
      parent_array[i].first = std::move(left_array[left_node->GetSize() - 1].first);
      old_array[0].second = left_array[left_node->GetSize() - 1].second;
      old_internal_page->IncreaseSize(1);
      left_node->IncreaseSize(-1);

      // 其实孩子0不管是internalpage还是leafpage，都一样，因为parentpageid保存在头信息中，
      // 所以哪怕是leafpage，我当做internalpage去处理也是一点问题没有的。
      InternalPage *child0 = FetchInternalPage(old_array[0].second);
      child0->SetParentPageId(old_internal_page->GetPageId());
      buffer_pool_manager_->UnpinPage(child0->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(left_node->GetPageId(), true);
      return true;
    }

    buffer_pool_manager_->UnpinPage(left_node->GetPageId(), false);
  }

  if (i + 1 <= parent->GetSize()) {
    InternalPage *right_node = FetchInternalPage(parent->ValueAt(i + 1));

    if (right_node->GetSize() > internal_min_size_) {
      auto *right_array = right_node->GetArray();
      old_array[old_internal_page->GetSize()].first = parent_array[i + 1].first;
      old_array[old_internal_page->GetSize()].second = right_array[0].second;
      parent_array[i + 1].first = std::move(right_array[1].first);

      for (int i = 0; i < right_node->GetSize() - 1; ++i) {
        right_array[i] = std::move(right_array[i + 1]);
      }
      right_node->IncreaseSize(-1);
      old_internal_page->IncreaseSize(1);

      InternalPage *child0 = FetchInternalPage(old_array[old_internal_page->GetSize() - 1].second);
      child0->SetParentPageId(old_internal_page->GetPageId());
      buffer_pool_manager_->UnpinPage(child0->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(right_node->GetPageId(), true);
      return true;
    }

    buffer_pool_manager_->UnpinPage(right_node->GetPageId(), false);
  }

  // 合并：
  if (i - 1 >= 0) {
    InternalPage *left_node = FetchInternalPage(parent->ValueAt(i - 1));
    auto *left_array = left_node->GetArray();
    left_array[left_node->GetSize()].first = std::move(parent_array[i].first);
    left_array[left_node->GetSize()].second = old_array[0].second;

    {
      InternalPage *child0 = FetchInternalPage(old_array[0].second);
      child0->SetParentPageId(left_node->GetPageId());
      buffer_pool_manager_->UnpinPage(child0->GetPageId(), true);
    }

    for (int j = left_node->GetSize() + 1, k = 1; k < old_internal_page->GetSize(); ++j, ++k) {
      left_array[j] = std::move(old_array[k]);
      InternalPage *child = FetchInternalPage(left_array[j].second);
      child->SetParentPageId(left_node->GetPageId());
      buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
    }

    left_node->IncreaseSize(old_internal_page->GetSize());

    for (int j = i; j < parent->GetSize() - 1; ++j) {
      parent_array[j] = std::move(parent_array[j + 1]);
    }

    parent->IncreaseSize(-1);

    bool ret = parent->GetSize() >= internal_min_size_;

    if (parent->IsRootPage() && parent->GetSize() <= 1) {
      root_page_id_ = left_node->GetPageId();
      left_node->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(old_internal_page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(old_internal_page->GetPageId());
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      buffer_pool_manager_->DeletePage(parent->GetPageId());
      old_internal_page = left_node;
      return true;
    }

    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(old_internal_page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(old_internal_page->GetPageId());

    // 让它正常工作。
    old_internal_page = left_node;
    return ret;
  }

  if (i + 1 <= parent->GetSize()) {
    InternalPage *right_node = FetchInternalPage(parent->ValueAt(i + 1));
    auto *right_array = right_node->GetArray();
    old_array[old_internal_page->GetSize()].first = std::move(parent_array[i + 1].first);
    old_array[old_internal_page->GetSize()].second = right_array[0].second;

    {
      InternalPage *child0 = FetchInternalPage(right_array[0].second);
      child0->SetParentPageId(old_internal_page->GetPageId());
      buffer_pool_manager_->UnpinPage(child0->GetPageId(), true);
    }

    for (int j = old_internal_page->GetSize() + 1, k = 1; k < right_node->GetSize(); ++j, ++k) {
      old_array[j] = std::move(right_array[k]);
      InternalPage *child = FetchInternalPage(old_array[j].second);
      child->SetParentPageId(old_internal_page->GetPageId());
      buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
    }

    for (int j = i + 1; j < parent->GetSize(); ++j) {
      parent_array[j] = std::move(parent_array[j + 1]);
    }

    parent->IncreaseSize(-1);
    old_internal_page->IncreaseSize(right_node->GetSize());

    if (parent->IsRootPage() && parent->GetSize() <= 1) {
      root_page_id_ = old_internal_page->GetPageId();
      old_internal_page->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(right_node->GetPageId(), true);
      buffer_pool_manager_->DeletePage(right_node->GetPageId());
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      buffer_pool_manager_->DeletePage(parent->GetPageId());
      return true;
    }

    bool ret = parent->GetSize() >= internal_min_size_;

    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(right_node->GetPageId());

    return ret;
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeLeaf(LeafPage *&old_leaf_page) -> bool {
  if (old_leaf_page->GetSize() >= leaf_min_size_) {
    return true;
  }
  if (old_leaf_page->IsRootPage()) {
    if (old_leaf_page->GetSize() <= 0) {
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      buffer_pool_manager_->DeletePage(root_page_id_);
      root_page_id_ = INVALID_PAGE_ID;
      begin_id_ = INVALID_PAGE_ID;
    }
    return true;
  }

  InternalPage *parent = FetchInternalPage(old_leaf_page->GetParentPageId());
  auto *parent_array = parent->GetArray();
  auto *old_array = old_leaf_page->GetArray();

  // i是old_leaf_page在下面的下标。
  int i = UpperBound(parent_array + 1, parent_array + parent->GetSize(), old_array[0].first) - parent_array - 1;

  // 借孩子：
  if (i - 1 >= 0) {
    LeafPage *leaf_node = FetchLeafPage(parent->ValueAt(i - 1));

    if (leaf_node->GetSize() > leaf_min_size_) {
      auto *leaf_array = leaf_node->GetArray();

      for (int j = old_leaf_page->GetSize(); j > 0; --j) {
        old_array[j] = std::move(old_array[j - 1]);
      }

      old_array[0] = std::move(leaf_array[leaf_node->GetSize() - 1]);
      leaf_node->IncreaseSize(-1);
      old_leaf_page->IncreaseSize(1);
      parent_array[i].first = old_array[0].first;

      buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      return true;
    }

    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  }

  if (i + 1 < parent->GetSize()) {
    LeafPage *right_node = FetchLeafPage(parent->ValueAt(i + 1));

    if (right_node->GetSize() > leaf_min_size_) {
      auto *right_array = right_node->GetArray();

      old_array[old_leaf_page->GetSize()] = std::move(right_array[0]);
      old_leaf_page->IncreaseSize(1);
      right_node->IncreaseSize(-1);

      for (int j = 0; j <= right_node->GetSize(); ++j) {
        right_array[j] = std::move(right_array[j + 1]);
      }

      parent_array[i + 1].first = right_array[0].first;

      buffer_pool_manager_->UnpinPage(right_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      return true;
    }

    buffer_pool_manager_->UnpinPage(right_node->GetPageId(), false);
  }

  // 合并操作：
  if (i - 1 >= 0) {
    LeafPage *left_node = FetchLeafPage(parent->ValueAt(i - 1));
    auto *left_array = left_node->GetArray();

    for (int j = left_node->GetSize(), k = 0; k < old_leaf_page->GetSize(); ++j, ++k) {
      left_array[j] = std::move(old_array[k]);
    }
    left_node->IncreaseSize(old_leaf_page->GetSize());

    // 在parent删除i
    for (int j = i; j + 1 < parent->GetSize(); ++j) {
      parent_array[j] = std::move(parent_array[j + 1]);
    }
    parent->IncreaseSize(-1);

    left_node->SetNextPageId(old_leaf_page->GetNextPageId());

    buffer_pool_manager_->UnpinPage(old_leaf_page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(old_leaf_page->GetPageId());

    if (parent->IsRootPage() && parent->GetSize() <= 1) {
      left_node->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = left_node->GetPageId();
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      buffer_pool_manager_->DeletePage(parent->GetPageId());
      return true;
    }

    bool ret = parent->GetSize() >= internal_min_size_;

    // 更改当前位置，让下面的while(!MergeLeaf())可以正常工作。
    old_leaf_page = left_node;

    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return ret;
  }

  if (i + 1 < parent->GetSize()) {
    LeafPage *right_node = FetchLeafPage(parent->ValueAt(i + 1));
    auto *right_array = right_node->GetArray();

    for (int j = old_leaf_page->GetSize(), k = 0; k < right_node->GetSize(); ++j, ++k) {
      old_array[j] = std::move(right_array[k]);
    }

    old_leaf_page->IncreaseSize(right_node->GetSize());

    // 在parent删除 i + 1
    for (int j = i + 1; j + 1 < parent->GetSize(); ++j) {
      parent_array[j] = std::move(parent_array[j + 1]);
    }
    parent->IncreaseSize(-1);

    old_leaf_page->SetNextPageId(right_node->GetNextPageId());

    buffer_pool_manager_->UnpinPage(right_node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(right_node->GetPageId());

    if (parent->IsRootPage() && parent->GetSize() <= 1) {
      old_leaf_page->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = old_leaf_page->GetPageId();
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      buffer_pool_manager_->DeletePage(parent->GetPageId());
      return true;
    }

    bool ret = parent->GetSize() >= internal_min_size_;

    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return ret;
  }

  return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 *
 * 返回唯一value带着input key。
 * 这个模仿用于点单点询问。
 * 返回：true如果key存在。
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  std::shared_lock<std::shared_mutex> lock(latch_);
  if (IsEmpty()) {
    return false;
  }

  // cur_page是当前页面，InternalPage是树的页面。
  InternalPage *cur_internal_page = FetchInternalPage(root_page_id_);
  int i = -1;

  // printf("getvalue : %s \n",key.data_);
  // 一直找到叶子。
  while (!cur_internal_page->IsLeafPage()) {
    auto *cur_array = cur_internal_page->GetArray();
    i = UpperBound(cur_array + 1, cur_array + cur_internal_page->GetSize(), key) - cur_array - 1;
    auto tmp = static_cast<page_id_t>(cur_internal_page->ValueAt(i));
    buffer_pool_manager_->UnpinPage(cur_internal_page->GetPageId(), false);
    cur_internal_page = FetchInternalPage(tmp);
  }

  auto *leaf_page = reinterpret_cast<LeafPage *>(cur_internal_page);
  cur_internal_page = nullptr;

  auto *leaf_array = leaf_page->GetArray();
  i = LowerBound(leaf_array, leaf_array + leaf_page->GetSize(), key) - leaf_array;
  // 因为叶子节点是从0开始，所以是小于号。
  if (i >= 0 && i < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(i), key) == 0) {
    result->push_back(leaf_page->ValueAt(i));
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return true;
  }

  // 草拟吗，这里忘记unpin了，导致内存用完了。之后出错了。、
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return false;
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
 *
 * 插入键值对到B+树中
 * 如果树空，建一个新树，更新根id并插入指针，否则插入叶子结点。
 * 返回值：因为我们只支持不重复键，所以如果用户试图插入一个重复键，则返回false，否则返回true、。
 *
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  std::scoped_lock<std::shared_mutex> lock(latch_);
  if (IsEmpty()) {
    // 申请页面并转化。
    LeafPage *new_leaf_page = NewLeafPage(INVALID_PAGE_ID);
    new_leaf_page->SetNextPageId(INVALID_PAGE_ID);
    new_leaf_page->SetSize(1);
    root_page_id_ = new_leaf_page->GetPageId();
    begin_id_ = root_page_id_;

    // 修改数据。
    auto *array = new_leaf_page->GetArray();
    array[0] = std::make_pair(key, value);

    // 别忘了Unpin
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }

  // printf("insert : %s\n",key.data_);
  InternalPage *cur_internal_page = FetchInternalPage(root_page_id_);
  int i = -1;

  // 直到叶子节点。
  while (!cur_internal_page->IsLeafPage()) {
    auto *cur_array = cur_internal_page->GetArray();
    i = UpperBound(cur_array + 1, cur_array + cur_internal_page->GetSize(), key) - cur_array - 1;
    auto tmp = static_cast<page_id_t>(cur_internal_page->ValueAt(i));

    buffer_pool_manager_->UnpinPage(cur_internal_page->GetPageId(), false);
    cur_internal_page = FetchInternalPage(tmp);
  }

  auto *leaf_page = reinterpret_cast<LeafPage *>(cur_internal_page);

  cur_internal_page = nullptr;

  // 寻找位置，
  auto *leaf_array = leaf_page->GetArray();
  i = LowerBound(leaf_array, leaf_array + leaf_page->GetSize(), key) - leaf_array;

  // 如果等于，返回false。
  if (i >= 0 && i < leaf_page->GetSize() && comparator_(key, leaf_page->KeyAt(i)) == 0) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }

  auto *array = leaf_page->GetArray();

  // 因为leaf从0开始，所以j从leaf_page->getsize()开始。
  for (int j = leaf_page->GetSize(); j > i; --j) {
    array[j] = std::move(array[j - 1]);
  }

  // 插入叶子结点，别忘了size+1
  array[i].first = key;
  array[i].second = value;
  leaf_page->IncreaseSize(1);

  // 叶子分裂完成且成功之后，直接返回true，比忘了UnpinImp
  if (SplitLeaf(leaf_page)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }

  // 找叶子的父亲，
  {
    page_id_t tmp = leaf_page->GetParentPageId();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    cur_internal_page = FetchInternalPage(tmp);
    leaf_page = nullptr;
  }
  
  // 内部节点的分裂。
  while (!SplitInternal(cur_internal_page)) {
    page_id_t tmp = cur_internal_page->GetParentPageId();
    buffer_pool_manager_->UnpinPage(cur_internal_page->GetPageId(), true);
    cur_internal_page = FetchInternalPage(tmp);
  }

  // 别忘了Unpin
  buffer_pool_manager_->UnpinPage(cur_internal_page->GetPageId(), true);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * 用输入的key删除键值对。
 * 如果树空，立刻返回。
 * 如果不是，用户需要首先寻找正确的叶子页面作为删除目标，然后删除指针。
 * 记得重新分配或者合并，如果需要的话。
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  std::scoped_lock<std::shared_mutex> lock(latch_);
  if (IsEmpty()) {
    return;
  }

  InternalPage *cur_internal_page = FetchInternalPage(root_page_id_);
  int i = -1;

  while (!cur_internal_page->IsLeafPage()) {
    auto *cur_array = cur_internal_page->GetArray();
    i = UpperBound(cur_array + 1, cur_array + cur_internal_page->GetSize(), key) - cur_array - 1;
    auto tmp = static_cast<page_id_t>(cur_internal_page->ValueAt(i));

    buffer_pool_manager_->UnpinPage(cur_internal_page->GetPageId(), false);
    cur_internal_page = FetchInternalPage(tmp);
  }

  auto *leaf_page = reinterpret_cast<LeafPage *>(cur_internal_page);
  cur_internal_page = nullptr;

  auto *leaf_array = leaf_page->GetArray();

  i = LowerBound(leaf_array, leaf_array + leaf_page->GetSize(), key) - leaf_array;

  if ((i >= 0 && i < leaf_page->GetSize() && comparator_(key, leaf_array[i].first) != 0) || i >= leaf_page->GetSize()) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }

  for (int j = i, n = leaf_page->GetSize() - 1; j != n; ++j) {
    leaf_array[j] = std::move(leaf_array[j + 1]);
  }

  leaf_page->IncreaseSize(-1);

  if (MergeLeaf(leaf_page)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return;
  }

  {
    page_id_t tmp = leaf_page->GetParentPageId();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    cur_internal_page = FetchInternalPage(tmp);
    leaf_page = nullptr;
  }

  while (!MergeInternal(cur_internal_page)) {
    page_id_t tmp = cur_internal_page->GetParentPageId();
    buffer_pool_manager_->UnpinPage(cur_internal_page->GetPageId(), true);
    cur_internal_page = FetchInternalPage(tmp);
  }

  // 别忘了Unpin
  buffer_pool_manager_->UnpinPage(cur_internal_page->GetPageId(), true);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(begin_id_, 0, buffer_pool_manager_); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }

  InternalPage *cur_internal_page = FetchInternalPage(root_page_id_);
  int i = -1;

  // 直到叶子节点。
  while (!cur_internal_page->IsLeafPage()) {
    auto *cur_array = cur_internal_page->GetArray();
    i = UpperBound(cur_array + 1, cur_array + cur_internal_page->GetSize(), key) - cur_array - 1;
    auto tmp = static_cast<page_id_t>(cur_internal_page->ValueAt(i));

    buffer_pool_manager_->UnpinPage(cur_internal_page->GetPageId(), false);
    cur_internal_page = FetchInternalPage(tmp);
  }

  auto *leaf_page = reinterpret_cast<LeafPage *>(cur_internal_page);

  cur_internal_page = nullptr;

  // 寻找位置，
  auto *leaf_array = leaf_page->GetArray();
  i = LowerBound(leaf_array, leaf_array + leaf_page->GetSize(), key) - leaf_array;

  // 如果等于，返回false。
  if (i >= 0 && i < leaf_page->GetSize() && comparator_(key, leaf_page->KeyAt(i)) == 0) {
    INDEXITERATOR_TYPE tmp(leaf_page->GetPageId(), i, buffer_pool_manager_);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return tmp;
  }

  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
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
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
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
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
