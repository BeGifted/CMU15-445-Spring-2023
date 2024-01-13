#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  auto cmp = [this](const Tuple &left_tuple, const Tuple &right_tuple) {
    for (auto &[type, expr] : plan_->GetOrderBy()) {
      auto left_value = expr->Evaluate(&left_tuple, this->child_executor_->GetOutputSchema());
      auto right_value = expr->Evaluate(&right_tuple, this->child_executor_->GetOutputSchema());
      if (left_value.CompareLessThan(right_value) == CmpBool::CmpTrue) {
        return type != OrderByType::DESC;
      }
      if (left_value.CompareGreaterThan(right_value) == CmpBool::CmpTrue) {
        return type == OrderByType::DESC;
      }
    }
    return true;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    pq.emplace(tuple);
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }
  while (!pq.empty()) {
    tuples_.emplace_back(pq.top());
    pq.pop();
  }
  it_ = tuples_.rbegin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ != tuples_.rend()) {
    *tuple = *(it_);
    it_++;
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return tuples_.rend() - it_; };

}  // namespace bustub
