/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "axiom/optimizer/tests/PrestoParser.h"
#include <algorithm>
#include <cctype>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ParserHelper.h"
#include "axiom/sql/presto/ast/AstBuilder.h"
#include "axiom/sql/presto/ast/AstPrinter.h"
#include "velox/exec/Aggregate.h"

namespace lp = facebook::velox::logical_plan;
namespace sql = axiom::sql::presto;

namespace facebook::velox::optimizer::test {
namespace {

// Analizes the expression to collect a list of dependent column names and
// number of aggregate function calls.
class ExprAnalyzer : public sql::AstVisitor {
 public:
  const std::unordered_set<std::string>& dependencies() const {
    return names_;
  }

  bool hasAggregate() const {
    return aggregateName_.has_value();
  }

  bool containsAny(const std::unordered_set<std::string>& names) const {
    for (const auto& name : names) {
      if (names_.contains(name)) {
        return true;
      }
    }
    return false;
  }

 private:
  void defaultVisit(sql::Node* node) override {
    if (dynamic_cast<sql::Literal*>(node) != nullptr) {
      // Literals have no dependencies.
      return;
    }

    VELOX_NYI(
        "Not yet supported node type: {}",
        sql::NodeTypeName::toName(node->type()));
  }

  void visitCast(sql::Cast* node) override {
    node->expression()->accept(this);
  }

  void visitDereferenceExpression(sql::DereferenceExpression* node) override {
    node->base()->accept(this);
  }

  void visitExtract(sql::Extract* node) override {
    node->expression()->accept(this);
  }

  void visitFunctionCall(sql::FunctionCall* node) override {
    const auto& name = node->name()->suffix();
    if (velox::exec::getAggregateFunctionEntry(name)) {
      if (aggregateName_.has_value()) {
      }
      VELOX_USER_CHECK(
          !aggregateName_.has_value(),
          "Cannot nest aggregations inside aggregation: {}({})",
          aggregateName_.value(),
          name);
      aggregateName_ = name;
    }

    for (const auto& arg : node->arguments()) {
      arg->accept(this);
    }
  }

  void visitArithmeticBinaryExpression(
      sql::ArithmeticBinaryExpression* node) override {
    node->left()->accept(this);
    node->right()->accept(this);
  }

  void visitLogicalBinaryExpression(
      sql::LogicalBinaryExpression* node) override {
    node->left()->accept(this);
    node->right()->accept(this);
  }

  void visitComparisonExpression(sql::ComparisonExpression* node) override {
    node->left()->accept(this);
    node->right()->accept(this);
  }

  void visitLikePredicate(sql::LikePredicate* node) override {
    node->value()->accept(this);
    node->pattern()->accept(this);
    if (node->escape() != nullptr) {
      node->escape()->accept(this);
    }
  }

  void visitSearchedCaseExpression(sql::SearchedCaseExpression* node) override {
    for (const auto& clause : node->whenClauses()) {
      clause->operand()->accept(this);
      clause->result()->accept(this);
    }

    if (node->defaultValue()) {
      node->defaultValue()->accept(this);
    }
  }

  void visitIdentifier(sql::Identifier* node) override {
    names_.insert(node->value());
  }

  std::unordered_set<std::string> names_;
  std::optional<std::string> aggregateName_;
};

class RelationPlanner : public sql::AstVisitor {
 public:
  explicit RelationPlanner(const std::string& defaultConnectorId)
      : context_{defaultConnectorId}, builder_(newBuilder()) {}

  lp::LogicalPlanNodePtr getPlan() {
    return builder_->build();
  }

 private:
  static std::string toFunctionName(sql::ComparisonExpression::Operator op) {
    switch (op) {
      case sql::ComparisonExpression::Operator::kEqual:
        return "eq";
      case sql::ComparisonExpression::Operator::kNotEqual:
        return "neq";
      case sql::ComparisonExpression::Operator::kLessThan:
        return "lt";
      case sql::ComparisonExpression::Operator::kLessThanOrEqual:
        return "lte";
      case sql::ComparisonExpression::Operator::kGreaterThan:
        return "gt";
      case sql::ComparisonExpression::Operator::kGreaterThanOrEqual:
        return "gte";
      default:
        VELOX_NYI("Not yet supported comparison operator: {}", op);
    }
  }

  static std::string toFunctionName(
      sql::ArithmeticBinaryExpression::Operator op) {
    switch (op) {
      case sql::ArithmeticBinaryExpression::Operator::kAdd:
        return "plus";
      case sql::ArithmeticBinaryExpression::Operator::kSubtract:
        return "minus";
      case sql::ArithmeticBinaryExpression::Operator::kMultiply:
        return "multiply";
      case sql::ArithmeticBinaryExpression::Operator::kDivide:
        return "divide";
      case sql::ArithmeticBinaryExpression::Operator::kModulus:
        return "modulus";
    }
  }

  static int32_t parseYearMonthInterval(
      const std::string& value,
      sql::IntervalLiteral::IntervalField start,
      std::optional<sql::IntervalLiteral::IntervalField> end) {
    VELOX_USER_CHECK(
        !end.has_value() || start == end.value(),
        "Multi-part intervals are not supported yet: {}",
        value);

    if (value.empty()) {
      return 0;
    }

    const auto n = atoi(value.c_str());

    switch (start) {
      case sql::IntervalLiteral::IntervalField::kYear:
        return n * 12;
      case sql::IntervalLiteral::IntervalField::kMonth:
        return n;
      default:
        VELOX_UNREACHABLE();
    }
  }

  static int64_t parseDayTimeInterval(
      const std::string& value,
      sql::IntervalLiteral::IntervalField start,
      std::optional<sql::IntervalLiteral::IntervalField> end) {
    VELOX_USER_CHECK(
        !end.has_value() || start == end.value(),
        "Multi-part intervals are not supported yet: {}",
        value);

    if (value.empty()) {
      return 0;
    }

    auto n = atol(value.c_str());

    switch (start) {
      case sql::IntervalLiteral::IntervalField::kDay:
        return n * 24 * 60 * 60;
      case sql::IntervalLiteral::IntervalField::kHour:
        return n * 60 * 60;
      case sql::IntervalLiteral::IntervalField::kMinute:
        return n * 60;
      case sql::IntervalLiteral::IntervalField::kSecond:
        return n;
      default:
        VELOX_UNREACHABLE();
    }
  }

  static lp::ExprApi parseDecimal(const std::string& value) {
    VELOX_USER_CHECK(!value.empty(), "Invalid decimal value: '{}'", value);

    size_t startPos = 0;
    if (value.at(0) == '+' || value.at(0) == '-') {
      startPos = 1;
    }

    int32_t periodPos = -1;
    int32_t firstNonZeroPos = -1;

    for (auto i = startPos; i < value.size(); ++i) {
      if (value.at(i) == '.') {
        VELOX_USER_CHECK_EQ(
            periodPos, -1, "Invalid decimal value: '{}'", value);
        periodPos = i;
      } else {
        VELOX_USER_CHECK(
            std::isdigit(value.at(i)), "Invalid decimal value: '{}'", value);

        if (firstNonZeroPos == -1 && value.at(i) != '0') {
          firstNonZeroPos = i;
        }
      }
    }

    size_t precision;
    size_t scale;
    std::string unscaledValue;

    if (periodPos == -1) {
      if (firstNonZeroPos == -1) {
        // All zeros: 000000. Treat as 0.
        precision = 1;
      } else {
        precision = value.size() - firstNonZeroPos;
      }

      scale = 0;
      unscaledValue = value;
    } else {
      scale = value.size() - periodPos - 1;

      if (firstNonZeroPos == -1 || firstNonZeroPos > periodPos) {
        // All zeros before decimal point. Treat as .0123.
        precision = scale > 0 ? scale : 1;
      } else {
        precision = value.size() - firstNonZeroPos - 1;
      }

      unscaledValue = value.substr(0, periodPos) + value.substr(periodPos + 1);
    }

    if (precision <= velox::ShortDecimalType::kMaxPrecision) {
      int64_t v = atol(unscaledValue.c_str());
      return lp::Lit(v, DECIMAL(precision, scale));
    }

    if (precision <= velox::LongDecimalType::kMaxPrecision) {
      return lp::Lit(
          folly::to<int128_t>(unscaledValue), DECIMAL(precision, scale));
    }

    VELOX_USER_FAIL(
        "Invalid decimal value: '{}'. Precision exceeds maximum: {} > {}.",
        value,
        precision,
        velox::LongDecimalType::kMaxPrecision);
  }

  static int32_t parseInt(const sql::TypeSignaturePtr& type) {
    VELOX_USER_CHECK_EQ(type->parameters().size(), 0);
    return atoi(type->baseName().c_str());
  }

  static TypePtr parseType(const sql::TypeSignaturePtr& type) {
    auto baseName = type->baseName();
    std::transform(
        baseName.begin(), baseName.end(), baseName.begin(), [](char c) {
          return (std::toupper(c));
        });

    if (baseName == "INT") {
      baseName = "INTEGER";
    }

    std::vector<TypeParameter> parameters;
    if (!type->parameters().empty()) {
      const auto numParams = type->parameters().size();
      parameters.reserve(numParams);

      if (baseName == "ARRAY") {
        VELOX_USER_CHECK_EQ(1, numParams);
        parameters.emplace_back(parseType(type->parameters().at(0)));
      } else if (baseName == "MAP") {
        VELOX_USER_CHECK_EQ(2, numParams);
        parameters.emplace_back(parseType(type->parameters().at(0)));
        parameters.emplace_back(parseType(type->parameters().at(1)));
      } else if (baseName == "ROW") {
        for (const auto& param : type->parameters()) {
          parameters.emplace_back(parseType(param), param->rowFieldName());
        }
      } else if (baseName == "DECIMAL") {
        VELOX_USER_CHECK_EQ(2, numParams);
        parameters.emplace_back(parseInt(type->parameters().at(0)));
        parameters.emplace_back(parseInt(type->parameters().at(1)));

      } else {
        VELOX_USER_FAIL("Unknown parametric type: {}", baseName);
      }
    }

    auto veloxType = getType(baseName, parameters);

    VELOX_CHECK_NOT_NULL(veloxType, "Cannot resolve type: {}", baseName);
    return veloxType;
  }

  bool asQualifiedName(
      const sql::ExpressionPtr& expr,
      std::vector<std::string>& names) {
    if (expr->is(sql::NodeType::kIdentifier)) {
      names.push_back(expr->as<sql::Identifier>()->value());
      return true;
    }

    if (expr->is(sql::NodeType::kDereferenceExpression)) {
      auto* dereference = expr->as<sql::DereferenceExpression>();
      names.push_back(dereference->field()->value());
      return asQualifiedName(dereference->base(), names);
    }

    return false;
  }

  lp::ExprApi toExpr(const sql::ExpressionPtr& node) {
    switch (node->type()) {
      case sql::NodeType::kIdentifier:
        return lp::Col(node->as<sql::Identifier>()->value());

      case sql::NodeType::kDereferenceExpression: {
        std::vector<std::string> names;
        if (asQualifiedName(node, names)) {
          VELOX_USER_CHECK_EQ(2, names.size());
          return lp::Col(names.at(0), lp::Col(names.at(1)));
        }

        auto* dereference = node->as<sql::DereferenceExpression>();
        return lp::Col(
            dereference->field()->value(), toExpr(dereference->base()));
      }

      case sql::NodeType::kSubqueryExpression: {
        auto* subquery = node->as<sql::SubqueryExpression>();
        auto query = subquery->query();

        if (query->is(sql::NodeType::kQuery)) {
          auto builder = std::move(builder_);

          builder_ = newBuilder();
          processQuery(query->as<sql::Query>());
          auto subqueryBuider = builder_;

          builder_ = std::move(builder);
          return lp::Subquery(subqueryBuider->build());
        }

        VELOX_NYI(
            "Subquery type is not supported yet: {}",
            sql::NodeTypeName::toName(query->type()));
      }

      case sql::NodeType::kComparisonExpression: {
        auto* comparison = node->as<sql::ComparisonExpression>();
        return lp::Call(
            toFunctionName(comparison->op()),
            toExpr(comparison->left()),
            toExpr(comparison->right()));
      }

      case sql::NodeType::kNotExpression: {
        auto* negation = node->as<sql::NotExpression>();
        return lp::Call("not", toExpr(negation->value()));
      }

      case sql::NodeType::kLikePredicate: {
        auto* like = node->as<sql::LikePredicate>();

        std::vector<lp::ExprApi> inputs;
        inputs.emplace_back(toExpr(like->value()));
        inputs.emplace_back(toExpr(like->pattern()));
        if (like->escape()) {
          inputs.emplace_back(toExpr(like->escape()));
        }

        return lp::Call("like", std::move(inputs));
      }

      case sql::NodeType::kLogicalBinaryExpression: {
        auto* logical = node->as<sql::LogicalBinaryExpression>();
        auto left = toExpr(logical->left());
        auto right = toExpr(logical->right());

        switch (logical->op()) {
          case sql::LogicalBinaryExpression::Operator::kAnd:
            return left && right;

          case sql::LogicalBinaryExpression::Operator::kOr:
            return left || right;
        }
      }

      case sql::NodeType::kArithmeticBinaryExpression: {
        auto* binary = node->as<sql::ArithmeticBinaryExpression>();
        return lp::Call(
            toFunctionName(binary->op()),
            toExpr(binary->left()),
            toExpr(binary->right()));
      }

      case sql::NodeType::kBetweenPredicate: {
        auto* between = node->as<sql::BetweenPredicate>();
        return lp::Call(
            "between",
            toExpr(between->value()),
            toExpr(between->min()),
            toExpr(between->max()));
      }

      case sql::NodeType::kInPredicate: {
        auto* inPredicate = node->as<sql::InPredicate>();

        auto inList = inPredicate->valueList()->as<sql::InListExpression>();

        std::vector<lp::ExprApi> inputs;
        inputs.reserve(1 + inList->values().size());

        inputs.emplace_back(toExpr(inPredicate->value()));
        for (const auto& expr : inList->values()) {
          inputs.emplace_back(toExpr(expr));
        }

        return lp::Call("in", inputs);
      }

      case sql::NodeType::kCast: {
        auto* cast = node->as<sql::Cast>();
        const auto type = parseType(cast->toType());

        if (cast->isSafe()) {
          return lp::TryCast(type, toExpr(cast->expression()));
        } else {
          return lp::Cast(type, toExpr(cast->expression()));
        }
      }

      case sql::NodeType::kSearchedCaseExpression: {
        auto* searchedCase = node->as<sql::SearchedCaseExpression>();

        std::vector<lp::ExprApi> inputs;
        inputs.reserve(1 + searchedCase->whenClauses().size());

        for (const auto& clause : searchedCase->whenClauses()) {
          inputs.emplace_back(toExpr(clause->operand()));
          inputs.emplace_back(toExpr(clause->result()));
        }

        if (searchedCase->defaultValue()) {
          inputs.emplace_back(toExpr(searchedCase->defaultValue()));
        }

        return lp::Call("switch", inputs);
      }

      case sql::NodeType::kExtract: {
        auto* extract = node->as<sql::Extract>();
        auto expr = toExpr(extract->expression());

        switch (extract->field()) {
          case sql::Extract::Field::kYear:
            return lp::Call("year", expr);
          case sql::Extract::Field::kQuarter:
            return lp::Call("quarter", expr);
          case sql::Extract::Field::kMonth:
            return lp::Call("month", expr);
          case sql::Extract::Field::kWeek:
            return lp::Call("week", expr);
          case sql::Extract::Field::kDay:
            [[fallthrough]];
          case sql::Extract::Field::kDayOfMonth:
            return lp::Call("day", expr);
          case sql::Extract::Field::kDow:
            [[fallthrough]];
          case sql::Extract::Field::kDayOfWeek:
            return lp::Call("day_of_week", expr);
          case sql::Extract::Field::kDoy:
            [[fallthrough]];
          case sql::Extract::Field::kDayOfYear:
            return lp::Call("day_of_year", expr);
          case sql::Extract::Field::kYow:
            [[fallthrough]];
          case sql::Extract::Field::kYearOfWeek:
            return lp::Call("year_of_week", expr);
          case sql::Extract::Field::kHour:
            return lp::Call("hour", expr);
          case sql::Extract::Field::kMinute:
            return lp::Call("minute", expr);
          case sql::Extract::Field::kSecond:
            return lp::Call("second", expr);
          case sql::Extract::Field::kTimezoneHour:
            return lp::Call("timezone_hour", expr);
          case sql::Extract::Field::kTimezoneMinute:
            return lp::Call("timezone_minute", expr);
        }
      }

      case sql::NodeType::kNullLiteral:
        return lp::Lit(Variant::null(TypeKind::UNKNOWN));

      case sql::NodeType::kLongLiteral:
        return lp::Lit(node->as<sql::LongLiteral>()->value());

      case sql::NodeType::kDoubleLiteral:
        return lp::Lit(node->as<sql::DoubleLiteral>()->value());

      case sql::NodeType::kDecimalLiteral:
        return parseDecimal(node->as<sql::DecimalLiteral>()->value());

      case sql::NodeType::kStringLiteral:
        return lp::Lit(node->as<sql::StringLiteral>()->value());

      case sql::NodeType::kIntervalLiteral: {
        const auto interval = node->as<sql::IntervalLiteral>();
        const int32_t multiplier =
            interval->sign() == sql::IntervalLiteral::Sign::kPositive ? 1 : -1;

        if (interval->isYearToMonth()) {
          const auto months = parseYearMonthInterval(
              interval->value(), interval->startField(), interval->endField());
          return lp::Lit(multiplier * months, INTERVAL_YEAR_MONTH());
        } else {
          const auto seconds = parseDayTimeInterval(
              interval->value(), interval->startField(), interval->endField());
          return lp::Lit(multiplier * seconds, INTERVAL_DAY_TIME());
        }
      }

      case sql::NodeType::kGenericLiteral: {
        auto literal = node->as<sql::GenericLiteral>();
        return lp::Cast(parseType(literal->type()), lp::Lit(literal->value()));
      }

      case sql::NodeType::kFunctionCall: {
        auto* call = node->as<sql::FunctionCall>();

        std::vector<lp::ExprApi> args;
        for (const auto& arg : call->arguments()) {
          args.push_back(toExpr(arg));
        }
        return lp::Call(call->name()->suffix(), args);
      }

      default:
        VELOX_NYI(
            "Unsupported expression type: {}",
            sql::NodeTypeName::toName(node->type()));
    }
  } // namespace

  void addFilter(const sql::ExpressionPtr& filter) {
    if (filter != nullptr) {
      builder_->filter(toExpr(filter));
    }
  }

  static lp::JoinType toJoinType(sql::Join::Type type) {
    switch (type) {
      case sql::Join::Type::kCross:
        return lp::JoinType::kInner;
      case sql::Join::Type::kImplicit:
        return lp::JoinType::kInner;
      case sql::Join::Type::kInner:
        return lp::JoinType::kInner;
      case sql::Join::Type::kLeft:
        return lp::JoinType::kLeft;
      case sql::Join::Type::kRight:
        return lp::JoinType::kRight;
      case sql::Join::Type::kFull:
        return lp::JoinType::kFull;
    }
  }

  void processFrom(const sql::RelationPtr& relation) {
    if (relation == nullptr) {
      // SELECT 1; type of query.
      builder_->values(ROW({}), {Variant::row({})});
      return;
    }

    if (relation->is(sql::NodeType::kTable)) {
      auto* table = relation->as<sql::Table>();
      builder_->tableScan(table->name()->suffix());
      return;
    }

    if (relation->is(sql::NodeType::kAliasedRelation)) {
      auto* aliasedRelation = relation->as<sql::AliasedRelation>();

      processFrom(aliasedRelation->relation());

      const auto& columnAliases = aliasedRelation->columnNames();
      if (!columnAliases.empty()) {
        // Add projection to rename columns.
        const size_t numColumns = columnAliases.size();

        std::vector<lp::ExprApi> renames;
        renames.reserve(numColumns);
        for (auto i = 0; i < numColumns; ++i) {
          renames.push_back(lp::Col(builder_->findOrAssignOutputNameAt(i))
                                .as(columnAliases.at(i)->value()));
        }

        builder_->project(renames);
      }

      builder_->as(aliasedRelation->alias()->value());
      return;
    }

    if (relation->is(sql::NodeType::kTableSubquery)) {
      auto* subquery = relation->as<sql::TableSubquery>();
      auto query = subquery->query();

      if (query->is(sql::NodeType::kQuery)) {
        processQuery(query->as<sql::Query>());
        return;
      }

      VELOX_NYI(
          "Subquery type is not supported yet: {}",
          sql::NodeTypeName::toName(query->type()));
    }

    if (relation->is(sql::NodeType::kJoin)) {
      auto* join = relation->as<sql::Join>();
      processFrom(join->left());

      auto leftBuilder = builder_;

      builder_ = newBuilder();
      processFrom(join->right());
      auto rightBuilder = builder_;

      builder_ = leftBuilder;

      std::optional<lp::ExprApi> condition;

      if (const auto& criteria = join->criteria()) {
        if (criteria->is(sql::NodeType::kJoinOn)) {
          condition = toExpr(criteria->as<sql::JoinOn>()->expression());
        } else {
          VELOX_NYI(
              "Join criteria type is not supported yet: {}",
              sql::NodeTypeName::toName(criteria->type()));
        }
      }

      builder_->join(*rightBuilder, condition, toJoinType(join->joinType()));
      return;
    }

    VELOX_NYI(
        "Relation type is not supported yet: {}",
        sql::NodeTypeName::toName(relation->type()));
  }

  // Returns true if 'selectItems' contains a single SELECT *.
  static bool isSelectAll(const std::vector<sql::SelectItemPtr>& selectItems) {
    if (selectItems.size() == 1 &&
        selectItems.at(0)->is(sql::NodeType::kAllColumns)) {
      return true;
    }

    return false;
  }

  void addProject(const std::vector<sql::SelectItemPtr>& selectItems) {
    std::vector<lp::ExprApi> exprs;
    for (const auto& item : selectItems) {
      VELOX_CHECK(item->is(sql::NodeType::kSingleColumn));
      auto* singleColumn = item->as<sql::SingleColumn>();

      lp::ExprApi expr = toExpr(singleColumn->expression());

      if (singleColumn->alias() != nullptr) {
        expr = expr.as(singleColumn->alias()->value());
      }
      exprs.push_back(expr);
    }

    builder_->project(exprs);
  }

  lp::ExprApi toSortingKey(const sql::ExpressionPtr& expr) {
    if (expr->is(sql::NodeType::kLongLiteral)) {
      const auto n = expr->as<sql::LongLiteral>()->value();
      const auto name = builder_->findOrAssignOutputNameAt(n - 1);

      return lp::Col(name);
    }

    return toExpr(expr);
  }

  lp::ExprApi toGroupingKey(const sql::ExpressionPtr& expr) {
    if (expr->is(sql::NodeType::kLongLiteral)) {
      const auto n = expr->as<sql::LongLiteral>()->value();
      const auto name = builder_->findOrAssignOutputNameAt(n);

      return lp::Col(name);
    }

    if (expr->is(sql::NodeType::kDereferenceExpression)) {
      std::vector<std::string> names;
      if (asQualifiedName(expr, names)) {
        VELOX_USER_CHECK_EQ(2, names.size());
        return lp::Col(names.at(0), lp::Col(names.at(1)));
      }
    }

    VELOX_CHECK_EQ(
        expr->type(),
        sql::NodeType::kIdentifier,
        "Grouping key must be a simple column reference. "
        "Expressions are not supported yet.");

    return toExpr(expr);
  }

  bool tryAddGlobalAgg(const std::vector<sql::SelectItemPtr>& selectItems) {
    bool hasAggregate = false;
    for (const auto& item : selectItems) {
      VELOX_CHECK(item->is(sql::NodeType::kSingleColumn));
      auto* singleColumn = item->as<sql::SingleColumn>();

      ExprAnalyzer exprAnalyzer;
      singleColumn->expression()->accept(&exprAnalyzer);

      if (exprAnalyzer.hasAggregate()) {
        hasAggregate = true;
        break;
      }
    }

    if (!hasAggregate) {
      return false;
    }

    // Verify that all expressions are aggregates or constant.
    std::vector<lp::ExprApi> aggregates;
    for (const auto& item : selectItems) {
      VELOX_CHECK(item->is(sql::NodeType::kSingleColumn));
      auto* singleColumn = item->as<sql::SingleColumn>();

      ExprAnalyzer exprAnalyzer;
      singleColumn->expression()->accept(&exprAnalyzer);

      lp::ExprApi expr = toExpr(singleColumn->expression());

      // TODO Allow constant expressions: SELECT 1 + 2, count(1) FROM t

      VELOX_USER_CHECK(
          exprAnalyzer.hasAggregate(),
          "[{}] must be an aggregate expression or appear in GROUP BY clause",
          expr.expr()->toString());

      if (singleColumn->alias() != nullptr) {
        expr = expr.as(singleColumn->alias()->value());
      }

      aggregates.push_back(expr);
    }

    builder_->aggregate({}, aggregates);
    return true;
  }

  void addGroupBy(
      const std::vector<sql::SelectItemPtr>& selectItems,
      const sql::GroupByPtr& groupBy) {
    VELOX_USER_CHECK(
        !groupBy->isDistinct(), "GROUP BY with DISTINCT is not supported yet");

    const auto& groupingElements = groupBy->groupingElements();

    // Collect grouping keys that refer to output columns. Add projection
    // if needed.
    std::vector<lp::ExprApi> groupingKeys;
    std::vector<int32_t> groupingKeyExprIndices;
    std::vector<int32_t> groupingKeyIndices(selectItems.size(), -1);
    {
      std::vector<lp::ExprApi> groupingKeyProjections;
      for (const auto& element : groupingElements) {
        VELOX_CHECK_EQ(element->type(), sql::NodeType::kSimpleGroupBy);

        const auto* simple = element->as<sql::SimpleGroupBy>();

        for (const auto& expr : simple->expressions()) {
          if (expr->is(sql::NodeType::kLongLiteral)) {
            // 1-based index.
            const auto n = expr->as<sql::LongLiteral>()->value();

            VELOX_CHECK_GE(n, 1);
            VELOX_CHECK_LE(n, selectItems.size());

            const auto& item = selectItems.at(n - 1);
            VELOX_CHECK(item->is(sql::NodeType::kSingleColumn));
            auto* singleColumn = item->as<sql::SingleColumn>();

            if (!singleColumn->expression()->is(sql::NodeType::kIdentifier)) {
              groupingKeyProjections.push_back(
                  toExpr(singleColumn->expression()));

              groupingKeyIndices[n - 1] = groupingKeys.size();

              groupingKeyExprIndices.push_back(groupingKeys.size());
              groupingKeys.push_back(lp::Lit(1)); // placeholder
            } else {
              groupingKeys.push_back(lp::Col(
                  singleColumn->expression()->as<sql::Identifier>()->value()));
            }
          } else {
            groupingKeys.push_back(toGroupingKey(expr));
          }
        }
      }

      if (!groupingKeyProjections.empty()) {
        const auto numOutput = builder_->numOutput();

        builder_->with(groupingKeyProjections);

        for (auto i = 0; i < groupingKeyExprIndices.size(); ++i) {
          groupingKeys[groupingKeyExprIndices[i]] =
              lp::Col(builder_->findOrAssignOutputNameAt(numOutput + i));
        }
      }
    }

    const auto numGroupingKeys = groupingKeys.size();

    // The following cases require a Project after Aggregation:
    // - SELECT contains expressions over grouping keys;
    // - The number or order of columns in SELECT doesn't match the output
    // of Aggregation.
    // - SELECT contains expressions over aggregate function calls.
    bool needsProjection = false;
    std::vector<lp::ExprApi> projections;

    // Grouping keys must be column references. Expressions are not
    // allowed yet.
    std::unordered_set<std::string> keyNames;
    for (const auto& key : groupingKeys) {
      VELOX_CHECK(key.name().has_value());
      keyNames.insert(key.name().value());
    }

    std::vector<lp::ExprApi> aggregates;
    std::vector<int32_t> aggregateIndices(selectItems.size(), -1);
    for (auto i = 0; i < selectItems.size(); ++i) {
      const auto& item = selectItems.at(i);

      VELOX_CHECK(item->is(sql::NodeType::kSingleColumn));
      auto* singleColumn = item->as<sql::SingleColumn>();

      if (groupingKeyIndices[i] != -1) {
        projections.push_back(groupingKeys[groupingKeyIndices[i]]);
        continue;
      }

      ExprAnalyzer exprAnalyzer;
      singleColumn->expression()->accept(&exprAnalyzer);

      lp::ExprApi expr = toExpr(singleColumn->expression());
      if (singleColumn->alias() != nullptr) {
        expr = expr.as(singleColumn->alias()->value());
      }

      if (exprAnalyzer.hasAggregate()) {
        aggregateIndices[i] = aggregates.size();
        projections.push_back(lp::Lit(1)); // placeholder

        aggregates.push_back(expr);
      } else {
        if (!singleColumn->expression()->is(sql::NodeType::kIdentifier)) {
          needsProjection = true;
        } else if (
            i >= numGroupingKeys || expr.name() != groupingKeys.at(i).name()) {
          needsProjection = true;
        }

        projections.push_back(expr);
      }
    }

    builder_->aggregate(groupingKeys, aggregates);

    if (selectItems.size() != numGroupingKeys + aggregates.size()) {
      needsProjection = true;
    }

    if (needsProjection) {
      for (auto i = 0; i < aggregateIndices.size(); ++i) {
        const auto index = aggregateIndices[i];
        if (index != -1) {
          const auto name =
              builder_->findOrAssignOutputNameAt(index + numGroupingKeys);
          projections[i] = lp::Col(name);
        }
      }

      builder_->project(projections);
    }
  }

  void addOrderBy(const sql::OrderByPtr& orderBy) {
    if (orderBy == nullptr) {
      return;
    }

    std::vector<lp::SortKey> keys;

    const auto& sortItems = orderBy->sortItems();
    for (const auto& item : sortItems) {
      auto expr = toSortingKey(item->sortKey());
      keys.emplace_back(expr, item->isAscending(), item->isNullsFirst());
    }

    builder_->sort(keys);
  }

  static int64_t parseInt64(const std::optional<std::string>& value) {
    return std::atol(value.value().c_str());
  }

  void addOffset(const sql::OffsetPtr& offset) {
    if (offset == nullptr) {
      return;
    }

    builder_->offset(std::atol(offset->offset().c_str()));
  }

  void addLimit(const std::optional<std::string>& limit) {
    if (!limit.has_value()) {
      return;
    }

    builder_->limit(parseInt64(limit));
  }

  void processQuery(sql::Query* query) {
    const auto& queryBody = query->queryBody();

    VELOX_CHECK_NOT_NULL(queryBody);
    VELOX_CHECK(queryBody->is(sql::NodeType::kQuerySpecification));
    auto* querySpec = queryBody->as<sql::QuerySpecification>();

    // FROM t -> builder.tableScan(t)
    processFrom(querySpec->from());

    // WHERE a > 1 -> builder.filter("a > 1")
    addFilter(querySpec->where());

    const auto& selectItems = querySpec->select()->selectItems();

    if (querySpec->groupBy()) {
      addGroupBy(selectItems, querySpec->groupBy());
      addFilter(querySpec->having());
    } else {
      if (isSelectAll(selectItems)) {
        // SELECT *. No project needed.
      } else if (tryAddGlobalAgg(selectItems)) {
        // Nothing else to do.
      } else {
        // SELECT a, b -> builder.project({a, b})
        addProject(selectItems);
      }
    }

    if (querySpec->select()->isDistinct()) {
      builder_->aggregate(builder_->findOrAssignOutputNames(), {});
    }

    addOrderBy(query->orderBy());
    addOffset(query->offset());
    addLimit(query->limit());
  }

  void visitQuery(sql::Query* query) override {
    processQuery(query);
  }

  void visitQuerySpecification(sql::QuerySpecification* node) override {}

  std::shared_ptr<lp::PlanBuilder> newBuilder() {
    return std::make_shared<lp::PlanBuilder>(
        context_, /* enableCoersions */ true);
  }

  lp::PlanBuilder::Context context_;
  std::shared_ptr<lp::PlanBuilder> builder_;
}; // namespace facebook::velox::optimizer::test

} // namespace

SqlStatementPtr PrestoParser::parseQuery(
    const std::string& sql,
    bool enableTracing) {
  return std::make_shared<SelectStatement>(doParse(sql, enableTracing));
}

lp::ExprPtr PrestoParser::parseExpression(
    const std::string& sql,
    bool enableTracing) {
  auto plan = doParse("SELECT " + sql, enableTracing);

  VELOX_USER_CHECK(plan->is(lp::NodeKind::kProject));

  auto project = plan->asUnchecked<lp::ProjectNode>();
  VELOX_CHECK_NOT_NULL(project);

  VELOX_USER_CHECK_EQ(1, project->expressions().size());
  return project->expressionAt(0);
}

logical_plan::LogicalPlanNodePtr PrestoParser::doParse(
    const std::string& sql,
    bool enableTracing) {
  sql::ParserHelper helper(sql);
  auto* queryContext = helper.parse();

  sql::AstBuilder astBuilder(enableTracing);
  auto query =
      astBuilder.visit(queryContext).as<std::shared_ptr<sql::Statement>>();

  if (enableTracing) {
    std::stringstream astString;
    sql::AstPrinter printer(astString);
    query->accept(&printer);

    std::cout << "AST: " << astString.str() << std::endl;
  }

  RelationPlanner planner(defaultConnectorId_);
  query->accept(&planner);

  return planner.getPlan();
}

} // namespace facebook::velox::optimizer::test
