//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/delete_executor.h"

#include <memory>

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  txn_ = exec_ctx_->GetTransaction();
  lock_mgr_ = exec_ctx_->GetLockManager();
  try {
    bool flag = lock_mgr_->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
    if (!flag) {
      throw ExecutionException("get table lock fail in delete\n");
    }
  } catch (...) {
    throw ExecutionException("get table lock fail in delete , may be it be killed \n");
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // 保证第一次返回true。
  if (finished_) {
    return false;
  }

  // 感觉应该是没有严格遵守火山模型。因为我直接删除了全部。
  auto *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  std::vector<IndexInfo *> indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  Tuple delete_tuple;
  RID delete_rid;
  int32_t delete_count = 0;

  while (child_executor_->Next(&delete_tuple, &delete_rid)) {
    try {
      bool flag = lock_mgr_->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, plan_->table_oid_, delete_rid);
      if (!flag) {
        throw ExecutionException("get row lock fail in delete\n");
      }
    } catch (...) {
      throw ExecutionException("get row lock fail in delete , maybe it be killed\n");
    }

    bool deleted = table_info->table_->MarkDelete(delete_rid, exec_ctx_->GetTransaction());

    if (deleted) {
      for (auto &it : indexs) {
        Tuple key =
            delete_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), it->key_schema_, it->index_->GetKeyAttrs());
        it->index_->DeleteEntry(key, delete_rid, exec_ctx_->GetTransaction());
      }

      ++delete_count;
    }
  }

  finished_ = true;
  *tuple = Tuple{std::vector<Value>(1, Value(TypeId::INTEGER, delete_count)), &GetOutputSchema()};
  return true;
}

}  // namespace bustub
