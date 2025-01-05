//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <functional>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "type/limits.h"

namespace bustub {

auto CheckLockMode(LockManager::LockMode lock_mode) -> std::string {
  switch (lock_mode) {
    case bustub::LockManager::LockMode::EXCLUSIVE:
      return "X";
    case bustub::LockManager::LockMode::INTENTION_EXCLUSIVE:
      return "IX";
    case bustub::LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      return "SIX";
    case bustub::LockManager::LockMode::SHARED:
      return "S";
    case bustub::LockManager::LockMode::INTENTION_SHARED:
      return "IS";
    default:
      return "";
  }
}

void LockManager::ThrowException(txn_id_t tid, AbortReason reason, int line) {
  auto tmp = TransactionAbortException(tid, reason);
  // printf("throw in line %d message : %s\n", line, tmp.GetInfo().c_str());
  throw tmp;
}

void LockManager::RowLockRemove(Transaction *txn, LockMode lock_mode, table_oid_t oid, const RID &rid) {
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
      break;
    case LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[oid].erase(rid);
      break;
    default:
      // BUSTUB_ASSERT(false, "row remove this lock is impossible\n");
      return;
  }
}

void LockManager::TableLockRemove(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    default:
      // BUSTUB_ASSERT(false, "table remove this lock is impossible\n");
      return;
  }
}

void LockManager::RowLockAllocate(Transaction *txn, LockMode lock_mode, table_oid_t oid, const RID &rid) {
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[oid].emplace(rid);
      return;
    case LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[oid].emplace(rid);
      return;
    default:
      break;
  }
  // BUSTUB_ASSERT(false, "row get this lock is impossible\n");
}

void LockManager::TableLockAllocate(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    default:
      // BUSTUB_ASSERT(false, "table get this lock is impossible\n");
      return;
  }
}

void LockManager::SetState(Transaction *txn, IsolationLevel ioslevel, LockMode lock_mode) {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    return;
  }
  // 意向锁不改变事务的状态。

  if (ioslevel == IsolationLevel::REPEATABLE_READ &&
      (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED)) {
    txn->SetState(TransactionState::SHRINKING);
    return;
  }

  if (lock_mode == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::SHRINKING);
  }
}

auto LockManager::CheckLock(LockMode lock_mode_, LockMode lock_mode) -> bool {
  // X，不兼容。
  if (lock_mode_ == LockMode::EXCLUSIVE) {
    return false;
  }

  // SIX之和IS兼容。
  if (lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode != LockMode::INTENTION_SHARED) {
    return false;
  }

  // S之和S与IS兼容。
  if (lock_mode_ == LockMode::SHARED && lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
    return false;
  }

  // IX之和IX与IS兼容。
  if (lock_mode_ == LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE &&
      lock_mode != LockMode::INTENTION_SHARED) {
    return false;
  }

  // IS不和X兼容。
  if (lock_mode_ == LockMode::INTENTION_SHARED && lock_mode == LockMode::EXCLUSIVE) {
    return false;
  }
  return true;
}

auto LockManager::GrantLock(std::shared_ptr<LockRequestQueue> &que, LockMode lock_mode,
                            std::shared_ptr<LockRequest> &lr, txn_id_t tid) -> bool {
  // 第一步，检查申请类型与已经granted_的是否兼容。
  // for (const auto &it : que->request_queue_) {
  //   if (!it->granted_) {
  //     continue;
  //   }

  //   if (!CheckLock(it->lock_mode_, lock_mode)) {
  //     return false;
  //   }
  // }
  // // 现在锁的类型是兼容的。
  // // 如果正在锁升级，则锁升级等级最高。
  // // 所以如果锁升级的不是当前，则false。
  // if (que->upgrading_ != INVALID_TXN_ID && que->upgrading_ != tid) {
  //   return false;
  // }

  // bool flag = true;

  // // 逻辑上有点错误： lock_mode 应该和队列前面的兼容。
  // // 但是当队列前面
  // for (const auto &it : que->request_queue_) {
  //   // printf("%d %d : %s\n", it->txn_id_, it->granted_,CheckLockMode(it->lock_mode_).c_str());
  //   if (it->txn_id_ == lr->txn_id_) {
  //     return flag;
  //   }

  //   if (!it->granted_) {
  //     flag = false;
  //   }
  //   // 现在it是等待的节点。
  //   if (!CheckLock(it->lock_mode_, lock_mode)) {
  //     return false;
  //   }
  // }

  // return false;
  for (auto &i : que->request_queue_) {
    if (i->granted_ && !CheckLock(i->lock_mode_, lock_mode)) {
      return false;
    }
  }

  for (auto &i : que->request_queue_) {
    if (!i->granted_) {
      for (auto &j : que->request_queue_) {
        if (i == j) {
          break;
        }

        if (!CheckLock(i->lock_mode_, j->lock_mode_)) {
          return false;
        }
      }
    }

    if (i->txn_id_ == tid) {
      return true;
    }
  }

  return false;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // try {
  // printf("tid : %d lock_mode : %s oid : %d\n", txn->GetTransactionId(), CheckLockMode(lock_mode).c_str(), oid);

  TransactionState state = txn->GetState();
  IsolationLevel ioslevel = txn->GetIsolationLevel();
  txn_id_t id = txn->GetTransactionId();

  // 第一步，检查txn的状态.
  {
    // ABORTED或者COMMITTED，返回false。
    if (state == TransactionState::ABORTED) {
      // ThrowException(id, AbortReason::LOCK_ON_SHRINKING);
      // throw "ABORTED or COMMITED but lock\n";

      // printf("%d has commited or aborted\n", id);
      return false;
    }

    // READ_UNCOMMITTED 只要是读锁，抛出异常。
    if (ioslevel == IsolationLevel::READ_UNCOMMITTED &&
        (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
         lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);

      ThrowException(id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED, 212);
      return false;
    }

    // 收缩阶段
    if (state == TransactionState::SHRINKING) {
      // REPEATABLE_READ不能申请锁。
      // 因为前面先判断了READ_UNCOMMITTED，所以这里可以直接返回。
      if (ioslevel == IsolationLevel::REPEATABLE_READ || ioslevel == IsolationLevel::READ_UNCOMMITTED) {
        // throw TransactionAbortException(id, AbortReason::LOCK_ON_SHRINKING);
        txn->SetState(TransactionState::ABORTED);

        ThrowException(id, AbortReason::LOCK_ON_SHRINKING, 224);
        return false;
      }

      // READ_COMMITTED只能申请S锁和IS锁。
      if (ioslevel == IsolationLevel::READ_COMMITTED && lock_mode != LockMode::SHARED &&
          lock_mode != LockMode::INTENTION_SHARED) {
        // throw TransactionAbortException(id, AbortReason::LOCK_ON_SHRINKING);

        txn->SetState(TransactionState::ABORTED);

        ThrowException(id, AbortReason::LOCK_ON_SHRINKING, 235);
        return false;
      }

      // BUSTUB_ASSERT(false, "Impossible IsolationLevel\n");
    }
  }

  // 现在，锁事务一定处于可以申请锁的阶段。
  // 可以开始尝试获取锁。

  // 第二步，获取table对应的lock request queue。
  table_lock_map_latch_.lock();

  if (table_lock_map_.count(oid) == 0) {
    std::shared_ptr<LockRequestQueue> tmp(new LockRequestQueue);
    table_lock_map_[oid] = tmp;
  }

  auto &que = table_lock_map_[oid];

  std::unique_lock<std::mutex> lock(que->latch_);

  // table锁在申请到queue的锁之后在释放。
  table_lock_map_latch_.unlock();
  bool upgrade = false;

  // 第三步，检查是否为锁升级。
  {
    // 寻找tid相同的事务。
    auto iter = std::find_if(que->request_queue_.begin(), que->request_queue_.end(),
                             [id](std::shared_ptr<LockRequest> &lr) -> bool { return lr->txn_id_ == id; });

    // 存在，且这个时候granted_一定为true；
    if (iter != que->request_queue_.end()) {
      // BUSTUB_ASSERT((*iter)->granted_, "granted should be true\n");

      if ((*iter)->lock_mode_ == lock_mode) {
        return true;
      }

      // 现在可以确定是锁升级
      // 先排除锁升级冲突。
      if (que->upgrading_ != INVALID_TXN_ID && que->upgrading_ != id) {
        //
        // throw TransactionAbortException(id, AbortReason::UPGRADE_CONFLICT);
        txn->SetState(TransactionState::ABORTED);

        ThrowException(id, AbortReason::UPGRADE_CONFLICT, 283);
        return false;
      }

      // 排除反向升级。
      bool is = txn->IsTableIntentionSharedLocked(oid);
      bool s = txn->IsTableSharedLocked(oid);
      bool ix = txn->IsTableIntentionExclusiveLocked(oid);
      bool six = txn->IsTableSharedIntentionExclusiveLocked(oid);

      // 如果不是以下这几个：则反向升级，
      if ((!is || lock_mode == LockMode::INTENTION_SHARED) &&  // IS锁除了本身，都可以申请。
          (!s || (lock_mode != LockMode::EXCLUSIVE &&
                  lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&  // S锁可以申请X锁或者SIX锁
          (!ix || (lock_mode != LockMode::EXCLUSIVE &&
                   lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&  // IX锁可以申请X锁或者SIX锁。
          (!six || lock_mode != LockMode::EXCLUSIVE)) {                    // SIX锁可以申请X锁。
        // //
        // throw TransactionAbortException(id, AbortReason::INCOMPATIBLE_UPGRADE);
        txn->SetState(TransactionState::ABORTED);

        ThrowException(id, AbortReason::INCOMPATIBLE_UPGRADE, 303);
        return false;
      }

      // 现在我们可以进行锁升级了。
      // 首先释放先前持有的锁.之后，和一个普通的锁请求一样，放入队列。
      TableLockRemove(txn, (*iter)->lock_mode_, oid);
      upgrade = true;
      que->upgrading_ = id;
      que->request_queue_.erase(iter);
    }
  }

  // 第四步，将锁请求放入请求队列。
  std::shared_ptr<LockRequest> lr(new LockRequest(id, lock_mode, oid));
  que->request_queue_.push_back(lr);

  // 第五步，尝试获取锁。
  // 条件变量并不是某一个特定语言中的概念，而是操作系统中线程同步的一种机制。

  while (!GrantLock(que, lock_mode, lr, id)) {
    que->cv_.wait(lock);
    // printf("%d wake up\n", id);
    if (txn->GetState() == TransactionState::ABORTED) {
      for (auto it = que->request_queue_.begin(); it != que->request_queue_.end();) {
        if ((*it)->txn_id_ == id) {
          TableLockRemove(txn, (*it)->lock_mode_, (*it)->oid_);
          it = que->request_queue_.erase(it);
        } else {
          ++it;
        }
      }

      if (que->upgrading_ == id) {
        que->upgrading_ = INVALID_TXN_ID;
      }
      que->cv_.notify_all();
      // printf("%d be killed\n", id);
      // RemovePoint(txn, id);

      return false;
    }
  }

  lr->granted_ = true;

  TableLockAllocate(txn, lock_mode, oid);

  if (upgrade) {
    que->upgrading_ = INVALID_TXN_ID;
  }

  return true;
  // } catch (...) {
  //   printf("throw in tid : %d lock_mode : %s oid : %d \n", txn->GetTransactionId(), CheckLockMode(lock_mode).c_str(),
  //          oid);
  // }
  // return false;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // try {
  // printf("tid : %d unlock oid : %d\n", txn->GetTransactionId(), oid);
  table_lock_map_latch_.lock();

  if (table_lock_map_.count(oid) == 0) {
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();

    ThrowException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, 355);
    return false;
  }

  auto &que = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(que->latch_);
  table_lock_map_latch_.unlock();
  //
  txn_id_t id = txn->GetTransactionId();

  {
    // 超你妈的，这里逻辑不对.
    // for (auto &it : *txn->GetExclusiveRowLockSet()) {
    //   if (!it.second.empty()) {
    //     txn->SetState(TransactionState::ABORTED);

    //     ThrowException(id, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS, 368);
    //     return false;
    //   }
    // }

    if (!(*txn->GetExclusiveRowLockSet())[oid].empty()) {
      txn->SetState(TransactionState::ABORTED);

      ThrowException(id, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS, 368);
      return false;
    }

    if (!(*txn->GetSharedRowLockSet())[oid].empty()) {
      txn->SetState(TransactionState::ABORTED);

      ThrowException(id, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS, 376);
      return false;
    }
  }

  auto iter = std::find_if(que->request_queue_.begin(), que->request_queue_.end(),
                           [id](std::shared_ptr<LockRequest> &lr) { return lr->granted_ && lr->txn_id_ == id; });

  if (iter == que->request_queue_.end()) {
    //
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    txn->SetState(TransactionState::ABORTED);

    ThrowException(id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, 390);
    return false;
  }

  LockMode lock_mode = (*iter)->lock_mode_;
  SetState(txn, txn->GetIsolationLevel(), lock_mode);

  if (que->upgrading_ != INVALID_TXN_ID && que->upgrading_ == id) {
    que->upgrading_ = INVALID_TXN_ID;
  }

  if (iter != que->request_queue_.end()) {
    que->request_queue_.erase(iter);
  }
  TableLockRemove(txn, lock_mode, oid);
  que->cv_.notify_all();

  return true;
  // } catch (...) {
  //   printf("throw in tid : %d oid : %d \n", txn->GetTransactionId(), oid);
  // }
  // return false;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // try {
  // printf("tid : %d lock_mode : %s oid : %d rid : %s\n", txn->GetTransactionId(), CheckLockMode(lock_mode).c_str(),
  // oid,
  //       rid.ToString().c_str());

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  TransactionState state = txn->GetState();
  IsolationLevel ioslevel = txn->GetIsolationLevel();
  txn_id_t id = txn->GetTransactionId();

  // 第一步，检查txn的状态.
  {
    // ABORTED或者COMMITTED，返回false。
    if (state == TransactionState::ABORTED) {
      // printf("%d has commited or aborted\n", id);
      return false;
    }

    // 检查在表上是否已经有锁。
    bool is = txn->IsTableIntentionSharedLocked(oid);
    bool s = txn->IsTableSharedLocked(oid);
    bool ix = txn->IsTableIntentionExclusiveLocked(oid);
    bool six = txn->IsTableSharedIntentionExclusiveLocked(oid);
    bool x = txn->IsTableExclusiveLocked(oid);

    if (lock_mode == LockMode::SHARED && !(is || s || ix || six || x)) {
      // 忘记设置状态了。
      txn->SetState(TransactionState::ABORTED);

      ThrowException(id, AbortReason::TABLE_LOCK_NOT_PRESENT, 433);
      return false;
    }

    if (lock_mode == LockMode::EXCLUSIVE && !(x || ix || six)) {
      txn->SetState(TransactionState::ABORTED);

      ThrowException(id, AbortReason::TABLE_LOCK_NOT_PRESENT, 439);
      return false;
    }

    /*-----------------------------------------------------------------------------------------------------------*/
    if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
      //
      // throw TransactionAbortException(id, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
      txn->SetState(TransactionState::ABORTED);

      ThrowException(id, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW, 448);
      return false;
    }
    // READ_UNCOMMITTED 只要是读锁，抛出异常。
    if (ioslevel == IsolationLevel::READ_UNCOMMITTED && lock_mode == LockMode::SHARED) {
      //
      // throw TransactionAbortException(id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      txn->SetState(TransactionState::ABORTED);

      ThrowException(id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED, 457);
      return false;
    }

    // 收缩阶段
    if (state == TransactionState::SHRINKING) {
      // REPEATABLE_READ不能申请锁。
      // 因为前面先判断了READ_UNCOMMITTED，所以这里可以直接返回。
      if (ioslevel == IsolationLevel::REPEATABLE_READ || ioslevel == IsolationLevel::READ_UNCOMMITTED) {
        //
        // throw TransactionAbortException(id, AbortReason::LOCK_ON_SHRINKING);
        txn->SetState(TransactionState::ABORTED);

        ThrowException(id, AbortReason::LOCK_ON_SHRINKING, 469);
        return false;
      }

      // READ_COMMITTED在收缩状态ROW只能申请S锁。
      if (ioslevel == IsolationLevel::READ_COMMITTED && lock_mode != LockMode::SHARED) {
        //
        // throw TransactionAbortException(id, AbortReason::LOCK_ON_SHRINKING);
        txn->SetState(TransactionState::ABORTED);

        ThrowException(id, AbortReason::LOCK_ON_SHRINKING, 478);
        return false;
      }

      // BUSTUB_ASSERT(false, "Impossible IsolationLevel\n");
    }
  }
  // 现在，锁事务一定处于可以申请锁的阶段。
  // 可以开始尝试获取锁。

  // 第二步，获取table对应的lock request queue。
  row_lock_map_latch_.lock();

  if (row_lock_map_.count(rid) == 0) {
    std::shared_ptr<LockRequestQueue> tmp(new LockRequestQueue());
    row_lock_map_[rid] = tmp;
  }
  auto &que = row_lock_map_[rid];

  std::unique_lock<std::mutex> lock(que->latch_);
  row_lock_map_latch_.unlock();
  bool upgrade = false;

  // 第三步，检查是否为锁升级。
  {
    // 寻找tid一样的
    auto iter = std::find_if(que->request_queue_.begin(), que->request_queue_.end(),
                             [id](std::shared_ptr<LockRequest> &lr) -> bool { return lr->txn_id_ == id; });

    // 存在，且这个时候granted_一定为true；
    if (iter != que->request_queue_.end()) {
      // BUSTUB_ASSERT((*iter)->granted_, "granted should be true\n");

      if ((*iter)->lock_mode_ == lock_mode) {
        return true;
      }

      // 现在可以确定是锁升级
      // 先排除锁升级冲突。
      if (que->upgrading_ != INVALID_TXN_ID && que->upgrading_ != id) {
        //
        // throw TransactionAbortException(id, AbortReason::UPGRADE_CONFLICT);
        txn->SetState(TransactionState::ABORTED);

        ThrowException(id, AbortReason::UPGRADE_CONFLICT, 522);
        return false;
      }

      // 排除反向升级。

      // 如果不是以下这几个：则反向升级，
      if (lock_mode != LockMode::EXCLUSIVE || !txn->IsRowSharedLocked(oid, rid)) {  // SIX锁可以申请X锁。
        //
        // throw TransactionAbortException(id, AbortReason::INCOMPATIBLE_UPGRADE);
        txn->SetState(TransactionState::ABORTED);

        ThrowException(id, AbortReason::INCOMPATIBLE_UPGRADE, 533);
        return false;
      }

      // 现在我们可以进行锁升级了。
      // 首先释放先前持有的锁.之后，和一个普通的锁请求一样，放入队列。
      RowLockRemove(txn, (*iter)->lock_mode_, oid, rid);
      que->upgrading_ = id;
      upgrade = true;
      que->request_queue_.erase(iter);
    }
  }

  // 第四步，将锁请求放入请求队列。
  // 小溪了，这里应该是RID的请求。
  std::shared_ptr<LockRequest> lr(new LockRequest(id, lock_mode, oid, rid));
  que->request_queue_.push_back(lr);

  // 第五步，尝试获取锁。
  // 条件变量并不是某一个特定语言中的概念，而是操作系统中线程同步的一种机制。

  while (!GrantLock(que, lock_mode, lr, id)) {
    que->cv_.wait(lock);
    //  printf("%d wake up\n", id);
    if (txn->GetState() == TransactionState::ABORTED) {
      for (auto it = que->request_queue_.begin(); it != que->request_queue_.end();) {
        if ((*it)->txn_id_ == id) {
          RowLockRemove(txn, (*it)->lock_mode_, (*it)->oid_, rid);
          it = que->request_queue_.erase(it);
        } else {
          ++it;
        }
      }

      if (que->upgrading_ == id) {
        que->upgrading_ = INVALID_TXN_ID;
      }
      que->cv_.notify_all();
      // printf("%d be killed\n", id);
      // RemovePoint(txn, id);

      return false;
    }
  }

  lr->granted_ = true;

  if (upgrade) {
    que->upgrading_ = id;
  }

  RowLockAllocate(txn, lock_mode, oid, rid);

  return true;
  // } catch (...) {
  //   printf("throw in tid : %d lock_mode : %s oid : %d rid : %s \n", txn->GetTransactionId(),
  //          CheckLockMode(lock_mode).c_str(), oid, rid.ToString().c_str());
  // }
  // return false;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  // try {
  // printf("tid : %d unlock oid : %d rid : %s \n", txn->GetTransactionId(), oid, rid.ToString().c_str());
  row_lock_map_latch_.lock();

  if (row_lock_map_.count(rid) == 0) {
    txn->SetState(TransactionState::ABORTED);
    row_lock_map_latch_.unlock();

    ThrowException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, 584);
    return false;
  }
  auto &que = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(que->latch_);
  row_lock_map_latch_.unlock();
  txn_id_t id = txn->GetTransactionId();

  auto iter = std::find_if(que->request_queue_.begin(), que->request_queue_.end(),
                           [id](std::shared_ptr<LockRequest> &lr) { return lr->granted_ && lr->txn_id_ == id; });

  if (iter == que->request_queue_.end() && txn->GetState() != TransactionState::ABORTED &&
      txn->GetState() != TransactionState::COMMITTED) {
    //
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    txn->SetState(TransactionState::ABORTED);

    ThrowException(id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, 599);
    return false;
  }

  LockMode lock_mode = (*iter)->lock_mode_;
  SetState(txn, txn->GetIsolationLevel(), lock_mode);

  if (que->upgrading_ != INVALID_TXN_ID && que->upgrading_ == id) {
    que->upgrading_ = INVALID_TXN_ID;
  }

  if (iter != que->request_queue_.end()) {
    que->request_queue_.erase(iter);
  }
  que->cv_.notify_all();
  RowLockRemove(txn, lock_mode, oid, rid);

  return true;
  // } catch (...) {
  //   printf("throw in tid : %d oid : %d rid : %s \n", txn->GetTransactionId(), oid, rid.ToString().c_str());
  // }
  // return false;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  // printf("addedge %d -> %d \n", t1, t2);
  waits_for_[t1].emplace(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  // printf("removeedge %d -> %d \n", t1, t2);
  auto it = waits_for_.find(t1);
  it->second.erase(t2);
  if (it->second.empty()) {
    waits_for_.erase(it);
  }
}

void LockManager::CreateGraph() {
  row_lock_map_latch_.lock();
  table_lock_map_latch_.lock();
  // printf("row lock : \n");

  for (const auto &[t, que] : row_lock_map_) {
    std::unique_lock<std::mutex> lock(que->latch_);
    // printf("row : %s \n", t.ToString().c_str());
    for (auto &i : que->request_queue_) {
      // printf("%d/%d ", (i)->txn_id_, (i)->granted_);
      for (auto &j : que->request_queue_) {
        if (!i->granted_ && j->granted_) {
          AddEdge((i)->txn_id_, j->txn_id_);
        }
      }
    }
    // printf("\n");
  }
  // printf("--------------------------------\n");
  for (const auto &[t, que] : table_lock_map_) {
    std::unique_lock<std::mutex> lock(que->latch_);
    for (auto &i : que->request_queue_) {
      for (auto &j : que->request_queue_) {
        if (!i->granted_ && j->granted_) {
          AddEdge(i->txn_id_, j->txn_id_);
        }
      }
    }
  }

  table_lock_map_latch_.unlock();
  row_lock_map_latch_.unlock();
}

void LockManager::ShowGraph() {
  printf("wait for graph : \n");
  for (const auto &[k, v] : waits_for_) {
    for (const auto &it : v) {
      printf("%d -> %d\n", k, it);
    }
  }
  printf("--------------------------\n");
}

void LockManager::Abort(Transaction *txn) {}

void LockManager::ReleaseLocks(Transaction *txn) {}

void LockManager::RemovePoint(Transaction *txn, txn_id_t tid) {
  std::scoped_lock<std::mutex> lock1(row_lock_map_latch_);
  std::scoped_lock<std::mutex> lock2(table_lock_map_latch_);

  for (const auto &[t, que] : row_lock_map_) {
    std::unique_lock<std::mutex> lock(que->latch_);
    bool flag = false;

    for (auto i = que->request_queue_.begin(); i != que->request_queue_.end(); ++i) {
      if ((*i)->txn_id_ == tid) {
        flag = true;
      }

      for (auto &j : que->request_queue_) {
        if (j->granted_ && !(*i)->granted_ && (j->txn_id_ == tid || (*i)->txn_id_ == tid)) {
          RemoveEdge((*i)->txn_id_, j->txn_id_);
        }
      }
    }

    if (flag) {
      que->cv_.notify_all();
    }
    // std::list<std::list<std::shared_ptr<LockRequest>>::iterator> li;

    // for (auto i = que->request_queue_.begin(); i != que->request_queue_.end(); ++i) {
    //   if ((*i)->txn_id_ == tid) {
    //     li.emplace_back(i);
    //   }

    //   for (auto &j : que->request_queue_) {
    //     if (j->granted_ && !(*i)->granted_ && (j->txn_id_ == tid || (*i)->txn_id_ == tid)) {
    //       RemoveEdge((*i)->txn_id_, j->txn_id_);
    //     }
    //   }
    // }
    // if (que->upgrading_ == tid) {
    //   que->upgrading_ = INVALID_TXN_ID;
    // }

    // if (!li.empty()) {
    //   que->cv_.notify_all();
    // }
  }

  for (const auto &[t, que] : table_lock_map_) {
    std::unique_lock<std::mutex> lock(que->latch_);

    bool flag = false;

    for (auto i = que->request_queue_.begin(); i != que->request_queue_.end(); ++i) {
      if ((*i)->txn_id_ == tid) {
        flag = true;
      }

      for (auto &j : que->request_queue_) {
        if (j->granted_ && !(*i)->granted_ && (j->txn_id_ == tid || (*i)->txn_id_ == tid)) {
          RemoveEdge((*i)->txn_id_, j->txn_id_);
        }
      }
    }

    if (flag) {
      que->cv_.notify_all();
    }

    // std::list<std::list<std::shared_ptr<LockRequest>>::iterator> li;

    // for (auto i = que->request_queue_.begin(); i != que->request_queue_.end(); ++i) {
    //   if ((*i)->txn_id_ == tid) {
    //     li.emplace_back(i);
    //   }

    //   for (auto &j : que->request_queue_) {
    //     if (j->granted_ && !(*i)->granted_ && (j->txn_id_ == tid || (*i)->txn_id_ == tid)) {
    //       RemoveEdge((*i)->txn_id_, j->txn_id_);
    //     }
    //   }
    // }

    // if (que->upgrading_ == tid) {
    //   que->upgrading_ = INVALID_TXN_ID;
    // }

    // for (auto &iter : li) {
    //   que->request_queue_.erase(iter);
    // }

    // if (!li.empty()) {
    //   que->cv_.notify_all();
    // }

    // bool flag = std::any_of(que->request_queue_.begin(), que->request_queue_.end(),
    //                         [tid](std::shared_ptr<LockRequest> &lr) { return tid == lr->txn_id_; });

    // if (flag) {
    //   que->cv_.notify_all();
    // }
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  /*----------dfs判断环--------------------*/
  std::unordered_set<txn_id_t> txn_set;
  std::unordered_set<txn_id_t> tmp;

  std::function<bool(txn_id_t)> dfs = [&txn_set, this, &dfs, &tmp](txn_id_t now) -> bool {
    txn_set.emplace(now);
    tmp.emplace(now);
    auto &it = waits_for_[now];

    return std::any_of(it.begin(), it.end(),
                       [&txn_set, &dfs](txn_id_t it) -> bool { return txn_set.count(it) > 0 || dfs(it); });
  };

  bool flag = false;

  for (auto &[k, v] : waits_for_) {
    if (tmp.count(k) > 0) {
      continue;
    }

    if (dfs(k)) {
      flag = true;
      break;
    }

    txn_set.clear();
  }

  if (flag) {
    txn_id_t id = -1;
    for (txn_id_t i : txn_set) {
      id = id > i ? id : i;
    }
    *txn_id = id;
    return true;
  }

  // ShowGraph();
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;

  for (const auto &[k, list] : waits_for_) {
    for (const auto &it : list) {
      edges.emplace_back(k, it);
    }
  }

  return edges;
}

void LockManager::RunCycleDetection() {
  // enable_cycle_detection_ = false;
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {                 // TODO(students): detect deadlock
      CreateGraph();  // 建图
      txn_id_t tid;
      // ShowGraph();
      while (HasCycle(&tid)) {
        auto txn = TransactionManager::GetTransaction(tid);
        txn->SetState(TransactionState::ABORTED);
        RemovePoint(txn, tid);  // 在图中删除这个节点，并唤醒。
        // ShowGraph();
      }
      waits_for_.clear();
    }
  }
}

}  // namespace bustub
