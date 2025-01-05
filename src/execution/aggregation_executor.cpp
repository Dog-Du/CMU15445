//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/aggregation_executor.h"

#include <memory>
#include <vector>

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;

  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }

  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!finished_ && aht_iterator_ == aht_.End()) {
    // Tuple由group_bys和aggregates两部分组成，
    // 如果group_bys大于0，说明由group by，这个时候全部由aggregates组成，根据这部分判断是否需要直接返回空。
    // 如果是带有group by的，应该返回空，所以直接返回false，不管了。
    if (!plan_->GetGroupBys().empty()) {
      return false;
    }

    // 如果是不带有group by的。需要根据文档说的，count(*)返回0，其余返回null。
    std::vector<Value> values;
    const auto &types = plan_->GetAggregateTypes();
    values.reserve(types.size());

    // 下面是聚合类型，返回空值，构建Tuple
    for (size_t i = 0, n = types.size(); i < n; ++i) {
      switch (types[i]) {
        case AggregationType::CountStarAggregate:
          values.push_back(ValueFactory::GetIntegerValue(0));
          break;
        case AggregationType::MaxAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::CountAggregate:
        case AggregationType::SumAggregate:
          // 改成了通用的类型，而不是整型。
          values.push_back(ValueFactory::GetNullValueByType(plan_->output_schema_->GetColumn(i).GetType()));
          break;
      }
    }

    *tuple = Tuple{values, &GetOutputSchema()};
    finished_ = true;
    return true;
  }

  // 非空表达到end，正常操作即可。
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  std::vector<Value> values;
  values.reserve(plan_->GetAggregates().size() + plan_->GetGroupBys().size());

  // 非空表直接插入即可。
  values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
  *tuple = Tuple{values, &GetOutputSchema()};
  ++aht_iterator_;
  // std::cout << tuple->ToString(&GetOutputSchema()) << std::endl;
  finished_ = true;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
