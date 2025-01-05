//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

// 至于为什么把列表构造函数写成这样的狗屎我也忘了。
// right对应Inner，left对应out。这个太恶心了，搞半天才明白left，right及其对应的schema。
// 只要能明白schema在哪里找。这个就很简单，比起loopjoin简单。
// left对应child_executor的schema，因为左边用来循环
// right对应plan的innerschema，
// 用left查找到right之后，拼接即可。
NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      child_executor_(std::move(child_executor)),
      plan_(plan),
      left_table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetInnerTableOid())),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())),
      right_table_info_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  // std::cout << child_executor_->GetOutputSchema().ToString() << "   " << plan_->InnerTableSchema().ToString()
  //           << std::endl;
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Inner型：
  if (plan_->GetJoinType() == JoinType::INNER) {
    std::vector<RID> tmp_rid;
    Tuple left_tuple;
    RID left_rid;

    // 没有查找到就一直查找，因为不插入重复键，所以要么可以连接，要么直接不用管。
    while (child_executor_->Next(&left_tuple, &left_rid)) {
      // Tuple key = left_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info_->key_schema_,
      //                                     index_info_->index_->GetKeyAttrs());

      // index probe key.
      // 这里要注意，这里和KeyFromTuple不同，需要取出需要进行寻找的那个Value，而不是Key。
      // 因为用于equal-join，所以只需要寻找一个相同的即可。
      // plan保存着key_predicate，取出对应的Value去index中查找。
      Tuple key =
          Tuple{std::vector<Value>(1, plan_->key_predicate_->Evaluate(&left_tuple, child_executor_->GetOutputSchema())),
                &index_info_->key_schema_};

      tree_->ScanKey(key, &tmp_rid, exec_ctx_->GetTransaction());

      // 查找到了、
      if (tmp_rid.size() == 1) {
        Tuple right_tuple;
        right_table_info_->table_->GetTuple(tmp_rid.front(), &right_tuple, exec_ctx_->GetTransaction());

        std::vector<Value> values;

        // 两个schema，然后进行拼接。
        uint32_t n1 = child_executor_->GetOutputSchema().GetColumnCount();
        uint32_t n2 = plan_->InnerTableSchema().GetColumnCount();

        values.reserve(n1 + n2);

        for (uint32_t k = 0; k < n1; ++k) {
          values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), k));
        }

        for (uint32_t k = 0; k < n2; ++k) {
          values.push_back(right_tuple.GetValue(&plan_->InnerTableSchema(), k));
        }

        *tuple = Tuple{values, &GetOutputSchema()};
        return true;
      }
    }

    // 找完了
    return false;
  }

  // left_join :
  // 和Innerjoin差不多，就是如果左边没找到需要补充空。
  Tuple left_tuple{};
  RID left_rid;

  // 这里不需要while循环，因为必定返回tuple。
  if (!child_executor_->Next(&left_tuple, &left_rid)) {
    return false;
  }

  Tuple key =
      Tuple{std::vector<Value>(1, plan_->key_predicate_->Evaluate(&left_tuple, child_executor_->GetOutputSchema())),
            &index_info_->key_schema_};
  // child_executor的schema是left，plan的innerschema是right。
  // Tuple key = left_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info_->key_schema_,
  //                                     index_info_->index_->GetKeyAttrs());

  // std::cout << child_executor_->GetOutputSchema().ToString() << "    " << plan_->InnerTableSchema().ToString()
  //           << std::endl;
  std::vector<RID> tmp_rid;
  tree_->ScanKey(key, &tmp_rid, exec_ctx_->GetTransaction());
  // 拼装
  std::vector<Value> values;

  uint32_t n1 = child_executor_->GetOutputSchema().GetColumnCount();
  uint32_t n2 = plan_->InnerTableSchema().GetColumnCount();

  values.reserve(n1 + n2);

  for (uint32_t k = 0; k < n1; ++k) {
    values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), k));
  }

  // 没找到的情况：
  if (tmp_rid.empty()) {
    for (uint32_t k = 0; k < n2; ++k) {
      // 其实这里直接填整型就好，但是为了通用性，写了GetType（）.
      values.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(k).GetType()));
    }
  } else {
    Tuple right_tuple;
    right_table_info_->table_->GetTuple(tmp_rid.front(), &right_tuple, exec_ctx_->GetTransaction());
    for (uint32_t k = 0; k < n2; ++k) {
      values.push_back(right_tuple.GetValue(&plan_->InnerTableSchema(), k));
    }
  }

  *tuple = Tuple{values, &GetOutputSchema()};
  return true;
}
}  // namespace bustub
