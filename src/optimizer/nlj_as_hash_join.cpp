#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(children);
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    auto &nlj_plan = dynamic_cast<NestedLoopJoinPlanNode &>(*optimized_plan);

    auto com_expr = dynamic_cast<ComparisonExpression *>(nlj_plan.Predicate().get());
    if (com_expr != nullptr && com_expr->comp_type_ == ComparisonType::Equal) {
      auto left_expr = dynamic_cast<ColumnValueExpression *>(com_expr->children_[0].get());
      auto right_expr = dynamic_cast<ColumnValueExpression *>(com_expr->children_[1].get());
      if (left_expr != nullptr && right_expr != nullptr) {
        std::vector<AbstractExpressionRef> left_key_exprs;
        std::vector<AbstractExpressionRef> right_key_exprs;
        if (left_expr->GetTupleIdx() == 0) {
          left_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
          right_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(1, right_expr->GetColIdx(), right_expr->GetReturnType()));
        } else {
          right_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(1, left_expr->GetColIdx(), left_expr->GetReturnType()));
          left_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
        }
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(left_key_exprs),
                                                  std::move(right_key_exprs), nlj_plan.GetJoinType());
      }
    }

    auto lg_expr = dynamic_cast<LogicExpression *>(nlj_plan.Predicate().get());
    if (lg_expr != nullptr) {
      if (lg_expr->logic_type_ != LogicType::And) {
        return optimized_plan;
      }

      auto first_lg_expr = dynamic_cast<ComparisonExpression *>(lg_expr->children_[0].get());
      auto second_lg_expr = dynamic_cast<ComparisonExpression *>(lg_expr->children_[1].get());
      if (first_lg_expr == nullptr || second_lg_expr == nullptr || first_lg_expr->comp_type_ != ComparisonType::Equal ||
          second_lg_expr->comp_type_ != ComparisonType::Equal) {
        return optimized_plan;
      }

      std::vector<AbstractExpressionRef> left_key_exprs;
      std::vector<AbstractExpressionRef> right_key_exprs;

      auto left_expr = dynamic_cast<ColumnValueExpression *>(first_lg_expr->children_[0].get());
      auto right_expr = dynamic_cast<ColumnValueExpression *>(first_lg_expr->children_[1].get());
      if (left_expr != nullptr && right_expr != nullptr) {
        if (left_expr->GetTupleIdx() == 0) {
          left_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
          right_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(1, right_expr->GetColIdx(), right_expr->GetReturnType()));
        } else {
          right_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(1, left_expr->GetColIdx(), left_expr->GetReturnType()));
          left_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
        }
      }

      left_expr = dynamic_cast<ColumnValueExpression *>(second_lg_expr->children_[0].get());
      right_expr = dynamic_cast<ColumnValueExpression *>(second_lg_expr->children_[1].get());
      if (left_expr != nullptr && right_expr != nullptr) {
        if (left_expr->GetTupleIdx() == 0) {
          left_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
          right_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(1, right_expr->GetColIdx(), right_expr->GetReturnType()));
        } else {
          right_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(1, left_expr->GetColIdx(), left_expr->GetReturnType()));
          left_key_exprs.emplace_back(
              std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
        }
      }
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), std::move(left_key_exprs),
                                                std::move(right_key_exprs), nlj_plan.GetJoinType());
    }
  }

  return optimized_plan;
}

}  // namespace bustub
