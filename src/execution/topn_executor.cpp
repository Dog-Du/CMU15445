#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  // 写比较函数。这个部分和sort一模一样，因为堆的堆顶也在 [0] 位。
  auto comp = [this](const Tuple &lhs, const Tuple &rhs) {
    for (auto &[order_type, expr] : plan_->order_bys_) {
      auto l = expr->Evaluate(&lhs, this->GetOutputSchema());
      auto r = expr->Evaluate(&rhs, this->GetOutputSchema());
      auto comp1 = l.CompareLessThan(r);
      auto comp2 = r.CompareLessThan(l);

      if (comp1 == CmpBool::CmpFalse && comp2 == CmpBool::CmpFalse) {
        continue;
      }

      if (order_type == OrderByType::DEFAULT || order_type == OrderByType::ASC) {
        return comp1 == CmpBool::CmpTrue;
      }

      if (order_type == OrderByType::DESC) {
        return comp2 == CmpBool::CmpTrue;
      }

      BUSTUB_ASSERT(false, "comparion not exit");
    }

    // 这里可以相等，其实仔细一想sort哪里相等好像也没什么问题。
    // BUSTUB_ASSERT(false, "tuple eq");
    return true;
  };

  // 注意这里优先队列怎么传入自定义排序规则
  // 因为comp是一个函数，支持operator (), 所以需要做的就是把comp传入，让堆当做；私有成员
  // 但是因为是个模版，需要先传入这个函数的类型，用关键字decltype可以解决。
  // 但是只是把类型传入，他没办法自己构造，即使自己构造也没办法传入捕捉的this。
  // 看了heap的构造函数之后，发现用可以传入比较函数，这个时候传入即可。
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(comp)> tmp_heap(comp);

  Tuple tuple;
  RID rid;

  // 之后就简单了，
  while (child_executor_->Next(&tuple, &rid)) {
    tmp_heap.emplace(tuple);

    if (tmp_heap.size() > plan_->n_) {
      tmp_heap.pop();
    }
  }

  tuples_.reserve(tmp_heap.size());

  while (!tmp_heap.empty()) {
    tuples_.emplace_back(tmp_heap.top());
    tmp_heap.pop();
  }

  // 注意这里需要翻转一下。
  std::reverse(tuples_.begin(), tuples_.end());
  iter_ = tuples_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.end()) {
    return false;
  }

  *tuple = *(iter_++);
  return true;
}

}  // namespace bustub
