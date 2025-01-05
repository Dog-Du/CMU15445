//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  right_executor_->Init();
  left_executor_->Init();
  Tuple tmp_tuple{};
  RID tmp_rid{};

  // 因为需要每个左tuple都需要遍历一遍右tuple，所以需要保存右边得所有tuple。
  while (right_executor_->Next(&tmp_tuple, &tmp_rid)) {
    right_tuples_.push_back(tmp_tuple);
  }

  // j_保存上次停下的位置。
  j_ = right_tuples_.size();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->GetJoinType() == JoinType::INNER) {
    RID tmp_rid;

    while (true) {
      // 找到了底，返回false。
      if (j_ == right_tuples_.size() && !left_executor_->Next(&left_tuple_, &tmp_rid)) {
        return false;
      }

      // 有新的left tuple，重头开始。
      if (j_ == right_tuples_.size()) {
        j_ = 0;
      }

      while (j_ < right_tuples_.size()) {
        // 评估可否插入、
        auto match = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                     &right_tuples_[j_], right_executor_->GetOutputSchema());

        if (!match.IsNull() && match.GetAs<bool>()) {
          std::vector<Value> values;
          uint32_t n1 = left_executor_->GetOutputSchema().GetColumnCount();
          uint32_t n2 = right_executor_->GetOutputSchema().GetColumnCount();
          values.reserve(n1 + n2);

          for (uint32_t k = 0; k < n1; ++k) {
            values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), k));
          }

          for (uint32_t k = 0; k < n2; ++k) {
            values.push_back(right_tuples_[j_].GetValue(&right_executor_->GetOutputSchema(), k));
          }

          *tuple = Tuple{values, &GetOutputSchema()};
          ++j_;

          return true;
        }

        ++j_;
      }
    }
    return false;
  }

  // left_join :
  RID tmp_rid;

  while (true) {
    if (j_ == right_tuples_.size() && !left_executor_->Next(&left_tuple_, &tmp_rid)) {
      return false;
    }

    if (j_ == right_tuples_.size()) {
      j_ = 0;
    }

    // std::cout << "left_tuple : " << left_tuple_.ToString(&left_executor_->GetOutputSchema())
    //           << "  right_tuple : " << right_tuples_[j_].ToString(&right_executor_->GetOutputSchema()) << std::endl;

    size_t start = j_;

    while (j_ < right_tuples_.size()) {
      auto match = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuples_[j_],
                                                   right_executor_->GetOutputSchema());

      if (!match.IsNull() && match.GetAs<bool>()) {
        std::vector<Value> values;
        uint32_t n1 = left_executor_->GetOutputSchema().GetColumnCount();
        uint32_t n2 = right_executor_->GetOutputSchema().GetColumnCount();
        values.reserve(n1 + n2);

        for (uint32_t k = 0; k < n1; ++k) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), k));
        }

        for (uint32_t k = 0; k < n2; ++k) {
          values.push_back(right_tuples_[j_].GetValue(&right_executor_->GetOutputSchema(), k));
        }

        *tuple = Tuple{values, &GetOutputSchema()};
        ++j_;
        return true;
      }

      ++j_;
    }

    // 从头找到尾都没有找到，
    if (start == 0 && j_ == right_tuples_.size()) {
      std::vector<Value> values;
      uint32_t n1 = left_executor_->GetOutputSchema().GetColumnCount();
      uint32_t n2 = right_executor_->GetOutputSchema().GetColumnCount();
      values.reserve(n1 + n2);

      for (uint32_t k = 0; k < n1; ++k) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), k));
      }

      for (uint32_t k = 0; k < n2; ++k) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(k).GetType()));
      }

      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
  }

  return false;
}

}  // namespace bustub
