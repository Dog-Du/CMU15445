#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  // limit在上，sort在下。这两者都是只有一个孩子。
  // 因为只能访问孩子，所以传入的应该是limit的节点。

  // 后序遍历整个执行树。
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  // 新的执行计划。
  auto optimize_plan = plan->CloneWithChildren(std::move(children));

  // 转化一下即可。
  if (optimize_plan->GetType() == PlanType::Limit && optimize_plan->GetChildAt(0)->GetType() == PlanType::Sort) {
    auto limit = static_cast<LimitPlanNode *>(const_cast<AbstractPlanNode *>(optimize_plan.get()));
    auto sort = static_cast<SortPlanNode *>(const_cast<AbstractPlanNode *>(optimize_plan->GetChildAt(0).get()));

    // 构造新节点，然后返回。optimize_plan就不用继续管了，因为是shared_ptr，所以不用管。
    AbstractPlanNodeRef topn(new TopNPlanNode(optimize_plan->output_schema_,
                                              optimize_plan->GetChildAt(0)->GetChildAt(0), sort->GetOrderBy(),
                                              limit->GetLimit()));
    return topn;
  }

  return optimize_plan;
}

}  // namespace bustub
