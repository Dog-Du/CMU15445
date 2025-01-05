//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

// 在iterator的生存周期中，完全pin住这个leaf_page。
INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator() = default;

  explicit IndexIterator(page_id_t page_id, int pos, BufferPoolManager *buffer_pool_manager)
      : index_(pos), buffer_pool_manager_(buffer_pool_manager), leaf_page_(nullptr) {
    if (page_id != INVALID_PAGE_ID) {
      leaf_page_ = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(page_id)->GetData());
    }
  }

  ~IndexIterator() {
    // buffer可能在iterator之前析构。所以按照effective cpp所说，吞下异常。
    if (leaf_page_ != nullptr) {
      try {
        buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
      } catch (...) {
        printf("buffer_pool_manager destructs before iterator\n");
      }
    }
  }

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return itr.index_ == index_ && itr.buffer_pool_manager_ == buffer_pool_manager_ && itr.leaf_page_ == leaf_page_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(this->operator==(itr)); }

 private:
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

  // buffer_pool_manager标识树，leafpage标识叶子，index表示下标。
  int index_;
  BufferPoolManager *buffer_pool_manager_;
  LeafPage *leaf_page_;
  // add your own private member variables here
};

}  // namespace bustub
