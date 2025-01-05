#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;

  // sort违反了火山模型。
  // 先压入vector。
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }

  // 自定义排序规则。sort这里传入的不是模版，所以直接传函数就好了。
  std::sort(tuples_.begin(), tuples_.end(), [this](Tuple &lhs, Tuple &rhs) -> bool {
    // std::cout << lhs.ToString(&this->GetOutputSchema()) << "  " << rhs.ToString(&this->GetOutputSchema())
    // <<std::endl;
    for (auto &[order_type, expr] : plan_->order_bys_) {
      // 这里我本来以为是用EvaluateJoin，因为它是两个参数。
      // 但是打印expr.ToString()，之后发现，它是column_value_expression。
      // 所以实际上操作方法是，取出left tuple需要比较的Value。再取出right tuple需要比较的Value。
      // 然后两者Value比较，根据比较结果判断返回值。
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

      // if (match.IsNull()) {
      //   std::cout << "equal" << std::endl;
      //   continue;
      // }

      // std::cout<< "result : " << match.GetAs<bool>()<<std::endl;
      // return match.GetAs<bool>();

      // bool flag = match.GetAs<bool>();

      // if (order_type == OrderByType::DEFAULT || order_type == OrderByType::ASC) {
      //   std::cout<< "lhs "
      //   return flag;
      // }

      // if (order_type == OrderByType::DESC){
      //   return !flag;
      // }

      BUSTUB_ASSERT(false, "comparion not exit");
    }

    BUSTUB_ASSERT(false, "tuple eq");
    return true;
  });

  iter_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.end()) {
    return false;
  }

  *tuple = *(iter_++);
  return true;
}

}  // namespace bustub
