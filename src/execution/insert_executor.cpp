//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/insert_executor.h"

#include <memory>
#include "common/exception.h"
#include "concurrency/lock_manager.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

// child_executor虽然是AbstractExecutor*类型，但是实际上是ValuesExecutor类型，这是一种多态，但是没有注释的话，很难判断到底是什么。
// 只能通过打印来猜测，最后打开对应的实现文件。
// 里面保存了需要插入的值。
void InsertExecutor::Init() {
  child_executor_->Init();
  txn_ = exec_ctx_->GetTransaction();
  lock_mgr_ = exec_ctx_->GetLockManager();

  try {
    bool flag = lock_mgr_->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
    if (!flag) {
      throw ExecutionException("get table lock fail in insert\n");
    }
  } catch (...) {
    throw ExecutionException("get table lock fail in insert , may be it be killed \n");
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // 因为要求第一次调用必须返回true。
  if (finished_) {
    return false;
  }

  // exe_ctx_保存着catalog，可以获得index_info和table_info。
  // 用table_info和index_info可以获得表和index。index中又保存着对应table的名字，可以在catalog中查找table。
  // plan_里面保存着关于本次执行的相关内容。包括table_oid和schema。
  // 如果不知道怎么做，可以打印出来schema看看。
  // Tuple和schema是密不可分的，但是却又不能封装在一个类中。
  // Tuple保存着数据，schema保存着解释Tuple的方式。
  auto *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  std::vector<IndexInfo *> indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  Tuple insert_tuple;
  RID insert_rid;
  int32_t insert_count = 0;

  while (child_executor_->Next(&insert_tuple, &insert_rid)) {
    // 在表中可以直接插入tuple。

    try {
      bool flag = lock_mgr_->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, plan_->table_oid_, insert_rid);
      if (!flag) {
        throw ExecutionException("get row lock fail in insert\n");
      }
    } catch (...) {
      throw ExecutionException("get row lock fail in insert , maybe it be killed\n");
    }

    bool inserted = table_info->table_->InsertTuple(insert_tuple, &insert_rid, exec_ctx_->GetTransaction());

    if (inserted) {
      for (auto &it : indexs) {
        // 但是在index中不能直接插入tuple，因为index中保存的是(key, rid)对，所以要对insert_tuple用KeyFromTuple
        // 三个参数是tuple的schema（翻译成框架比较好吧），要取出key类型的schema，和要取出key类型的列组。
        Tuple key =
            insert_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), it->key_schema_, it->index_->GetKeyAttrs());
        it->index_->InsertEntry(key, insert_rid, exec_ctx_->GetTransaction());
      }
      ++insert_count;
    }
  }

  // 执行一个之后就可以设定为true,下次就直接返回false即可。
  finished_ = true;
  // tuple是一个整数tuple，只有一个元素是interge，
  // Tuple构造函数的两个参数，一个是std::vector<Value>()，一个是schema。
  // Value的构造函数非常多，利用C++重载实现不同类型而Value的类型一致。
  // 通过union实现，既做到让多种不同类型字节数一致又做到一定程度节省空间。
  // 而且便于维护，之后想要新建一个类型只需要就该就好。
  // 这是，工厂模式？
  *tuple = Tuple{std::vector<Value>(1, Value(TypeId::INTEGER, insert_count)), &GetOutputSchema()};
  return true;
}

}  // namespace bustub
