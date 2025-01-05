//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   *
   * 拿着一个锁请求的结构。
   * 可以是一个表或者一行的所请求。
   * 对于表的锁请求，rid对象可以被忽略。
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid) {}

    /** Txn_id of the txn requesting the lock */
    // 事务id
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    // 锁类型
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    // 如果是表表示表id，如果是行表示表上的行id。
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    // 对于表锁没有用，对于行锁来说有用
    RID rid_;
    /** Whether the lock has been granted or not */
    // 是否已授予锁定
    bool granted_{false};
  };

  class LockRequestQueue {
   public:
    /** List of lock requests for the same resource (table or row) */
    // 对于相同资源的锁请求。
    std::list<std::shared_ptr<LockRequest>> request_queue_;
    /** For notifying blocked transactions on this rid */
    // 用于通知此 rid 上的被阻止交易。是一个条件变量。
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction (if any) */
    // 升级事务的txn_id（如果有）
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */
    // 协调
    std::mutex latch_;

    // 自己动手写一个析构，帮我释放。
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   * 创建为死锁检测策略配置的新锁管理器
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
  }

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime, do not grant the lock and return false.
   *
   * 一般行为：
   *    LockTable（） 和 LockRow（） 都是阻塞方法;他们应该等到锁定被授予后再返回.
   *    如果事务在此期间中止，则不要授予锁定并返回 false。
   *
   * MULTIPLE TRANSACTIONS:
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests, all should be granted at the same time
   *    as long as FIFO is honoured.
   *
   * 多笔交易：
   *    LockManager 应为每个资源维护一个队列;应以 FIFO 方式向交易授予锁定。
   *    如果有多个兼容的锁定请求，只要遵守 FIFO，就应同时授予所有请求。
   *
   * SUPPORTED LOCK MODES:
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   *
   * 支持的锁定模式：
   *    表锁定应支持所有锁定模式。
   *    行锁定不应支持 Intention 锁定。尝试此操作应将 TransactionState 设置为
   *    ABORTED 并引发 TransactionAbortException （ATTEMPTED_INTENTION_LOCK_ON_ROW）
   *
   * ISOLATION LEVEL:
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   *
   * 隔离等级：
   *    根据 ISOLATION LEVEL，事务应尝试采用锁：
   *    - 仅当需要时，并且
   *    - 仅在允许的情况下
   *
   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *
   *    例如，在 READ_UNCOMMITTED 下不需要 S/IS/SIX 锁，任何此类尝试都应将 TransactionState 设置为 ABORTED
   *    并引发 TransactionAbortException（LOCK_SHARED_ON_READ_UNCOMMITTED）。
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *    同样，如果事务状态为 SHRINKING，则不允许对行进行 X/IX 锁定，任何此类尝试都应将 TransactionState 设置为 ABORTED
   *    并引发 TransactionAbortException （LOCK_ON_SHRINKING）。
   *
   *    REPEATABLE_READ:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        No locks are allowed in the SHRINKING state
   *
   *    REPEATABLE_READ：
   *     交易被要求申请所有锁。
   *     GROWING 状态下所有锁都被允许
   *     在 SHRINKING 状态下不允许使用任何锁
   *
   *    READ_COMMITTED:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        Only IS, S locks are allowed in the SHRINKING state
   *    READ_COMMITTED：
   *        交易需要采取所有锁。
   *        所有锁都允许处于 GROWING 状态
   *        只有 IS、S 锁允许处于 SHRINKING 状态
   *
   *
   *    READ_UNCOMMITTED:
   *        The transaction is required to take only IX, X locks.
   *        X, IX locks are allowed in the GROWING state.
   *        S, IS, SIX locks are never allowed
   *    READ_UNCOMMITTED：
   *        该交易只需要采用 IX、X 锁。
   *        X、IX 锁允许在 GROWING 状态下使用。
   *        S、IS、SIX 锁是不允许的
   *
   *
   * MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   * 多级锁定：
   *   在锁定行时，Lock（） 应确保事务在行所属的表上具有适当的锁。
   *   例如，如果尝试对一行进行独占锁定，则事务必须在表上保留 X、IX 或 SIX。
   *   如果表上不存在这样的锁，Lock（） 应将 TransactionState 设置为 ABORTED
   *   并引发 TransactionAbortException （TABLE_LOCK_NOT_PRESENT）
   *
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *
   * 锁升级：
   *    在已锁定的资源上调用 Lock（） 应具有以下行为：
   *    - 如果请求的锁定模式与当前持有的锁定模式相同，
   *    Lock（） 应该返回 true，因为它已经有锁了。
   *    - 如果请求的锁定模式不同，Lock（） 应该升级事务持有的锁。
   *
   *    A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
   *    正在升级的锁定请求应优先于同一资源上的其他等待锁定请求。
   *
   *    While upgrading, only the following transitions should be allowed:
   *    当升级时，只有以下事务被允许：
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *    任何其他升级都被认为是不合理的，这样的请求应该把 TransactionState 设置成 ABORTED，
   *    并且抛出 TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
   *    此外，只应允许一个事务升级其对给定资源的锁定。
   *    同一资源上的多个并发锁升级应将 TransactionState 设置为 ABORTED，
   *    并引发 TransactionAbortException （UPGRADE_CONFLICT）。
   *
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update its
   *    lock sets appropriately (check transaction.h)
   * 簿记：
   *    如果向事务授予了锁，锁管理器应适当地更新其锁集（检查 transaction.h）
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *
   * 一般行为：
   * UnlockTable（） 和 UnlockRow（） 都应该释放对资源的锁定并返回。
   * 两者都应确保事务当前锁定它试图解锁的资源。
   * 否则，LockManager 应将 TransactionState 设置为 ABORTED
   * 并引发 TransactionAbortException （ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD）
   *
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *
   * 此外，仅当事务未在该表上的任何行上保持锁定时，才应允许解锁该表。
   * 如果事务在表的行上保持锁定，则 Unlock 应将事务状态设置为 ABORTED
   * 并引发 TransactionAbortException （TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS）。
   *
   * Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   * 最后，解锁资源还应授予对资源的任何新锁定请求（如果可能）。
   *
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   *
   * 事务状态更新
   * 解锁应适当地更新交易状态（取决于隔离级别）
   * 只有解锁 S 或 X 锁才能更改交易状态。
   *
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *        解锁 S/X 锁应将事务状态设置为 SHRINKING
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state.
   *        解锁X锁应该把事务设置为SHRINKING
   *        解锁S锁不影响事务状态。
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   *        解锁X锁应该把事务设置成SHRINKING
   *        READ_UNCOMMITTED不允许使用S锁。
   *            在此隔离级别下解锁 S 锁时的行为未定义。
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   *
   * 簿记：
   * 资源解锁后，锁管理器应适当地更新事务的锁集（检查 transaction.h）
   */

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * 在给定lock_mode中获取table_oid_t的锁。
   * 如果交易已经在桌子上持有锁，请将锁升级到指定的lock_mode（如果可能）。
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   *
   * 在某些情况下，此方法应中止事务并引发 TransactionAbortException。
   *
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * 释放被事务所持的锁。
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   *
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * 根据给定lock_mode在一个rid上申请一个锁
   * 如果事务已经在row上获得了一个锁，更新锁根据给定lock_mode（如果可能）。
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool;

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;

 private:
  void ReleaseLocks(Transaction *txn);
  void Abort(Transaction *txn);
  void ShowGraph();
  void RemovePoint(Transaction *txn, txn_id_t tid);
  void CreateGraph();
  void RowLockRemove(Transaction *txn, LockMode lock_mode, table_oid_t oid, const RID &rid);
  void TableLockRemove(Transaction *txn, LockMode lock_mode, table_oid_t oid);
  void ThrowException(txn_id_t id, AbortReason reason, int line);
  void RowLockAllocate(Transaction *txn, LockMode lock_mode, table_oid_t oid, const RID &rid);
  void TableLockAllocate(Transaction *txn, LockMode lock_mode, table_oid_t oid);
  void SetState(Transaction *txn, IsolationLevel ioslevel, LockMode lock_mode);
  // 检查两者是否兼容。
  auto CheckLock(LockMode lock_mode_, LockMode lock_mode) -> bool;

  // auto GrantLock(std::shared_ptr<LockRequestQueue> &que, LockMode lock_mode, LockRequest *lr, table_oid_t id) ->
  // bool;
  auto GrantLock(std::shared_ptr<LockRequestQueue> &que, LockMode lock_mode, std::shared_ptr<LockRequest> &lr,
                 txn_id_t id) -> bool;

  /** Fall 2022 */
  /** Structure that holds lock requests for a given table oid */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;

  /** Waits-for graph representation. */
  // std::unordered_map<txn_id_t, int> tid_count_map_;
  std::map<txn_id_t, std::set<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;
};

}  // namespace bustub
