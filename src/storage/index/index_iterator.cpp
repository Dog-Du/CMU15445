/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */

// 用leaf_page==nullptr表示end
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return leaf_page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_page_->GetArray()[index_]; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (leaf_page_ == nullptr) {
    return *this;
  }

  if (index_ < leaf_page_->GetSize() - 1) {
    ++index_;
    return *this;
  }

  index_ = 0;
  page_id_t tmp = leaf_page_->GetNextPageId();
  buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);

  if (tmp == INVALID_PAGE_ID) {
    leaf_page_ = nullptr;
    return *this;
  }

  leaf_page_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(tmp)->GetData());
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
