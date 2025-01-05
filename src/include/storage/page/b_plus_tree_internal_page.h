//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))

/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 *
 * 保存n个被索引的键和n+1个孩子指针（page_id）在内部页面。
 * 指针PAGE_ID(i)指向一个子树，它们都满足： K(i) <= K < K(i+1)
 *
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * 注意：因为key的个数不等于孩子节点的个数，所以第一个key总是不合法的。那就是说，任何搜索或查找
 * 都应该忽略第一个key。
 *
 * 内部节点格式（key被升序排列）
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  // 必须调用初始化方法在创建一个新节点之后。
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;
  auto GetArray() const -> MappingType *;

 private:
  // Flexible array member for page data.
  // 自由的数组成员给页面信息。
  // MappingType array_[1]; 我想应该是这个数。
  // 它总是让一个节点占据一页，这样方便编写代码，也方便管理。
  MappingType array_[INTERNAL_PAGE_SIZE];
};
}  // namespace bustub
