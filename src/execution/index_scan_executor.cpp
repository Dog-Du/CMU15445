//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  // 文档上说，
  // The type of the index object in the plan will always be BPlusTreeIndexForOneIntegerColumn in this project.
  // You can safely cast it and store it in the executor object:
  // 然后就获得了裸指针。
  auto *tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  // 因为非聚簇索引，所以树上只有RID，没有数据，想获得数据，要先获得RID和table，然后朝着table要数据.
  // table_name 藏在tree上。
  table_info_ = exec_ctx_->GetCatalog()->GetTable(tree->GetMetadata()->GetTableName());
  iter_ = tree->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_.IsEnd()) {
    return false;
  }
  *rid = (*iter_).second;
  // 在table上gettuple通过rid。
  table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++iter_;
  return true;
}

}  // namespace bustub
