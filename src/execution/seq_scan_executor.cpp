//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/exception.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->table_oid_)),
      iter_(table_info_->table_->Begin(exec_ctx->GetTransaction())) {}

void SeqScanExecutor::Init() {
  txn_ = exec_ctx_->GetTransaction();
  lock_mgr_ = exec_ctx_->GetLockManager();

  if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    bool flag = lock_mgr_->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED, plan_->table_oid_);
    if (!flag) {
      throw ExecutionException("get line lock fail in seq_scan\n");
    }
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 很简单，现在回想起来。
  if (iter_ == table_info_->table_->End()) {
    return false;
  }

  if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    bool flag = lock_mgr_->LockRow(txn_, LockManager::LockMode::SHARED, plan_->table_oid_, iter_->GetRid());
    if (!flag) {
      throw ExecutionException("get row lock fail in seq_scan\n");
    }
  }

  *tuple = *iter_;
  *rid = iter_->GetRid();
  ++iter_;
  return true;
}

}  // namespace bustub
