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
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 *
 * 初始化方法，在创建一个新的内部页面之后
 * 包括设置页面类型，设置当前大小，设置页面id，设置父亲id，设置页面最大大小。
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 *
 * 用于获取/设置与输入“index”（又名数组偏移量）关联的键的辅助方法
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // // replace with your own code
  // KeyType key{};
  // return key;
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  // BUSTUB_ASSERT(index >= 1 && index <= GetSize(), "overflow in b_plus_tree_internal_page::SetKeyAt");
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // BUSTUB_ASSERT(index >= 1 && index <= GetSize(), "overflow in b_plus_tree_internal_page::ValueAt");
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetArray() const -> MappingType * { return const_cast<MappingType *>(array_); }

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
