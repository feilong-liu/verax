/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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

#include "axiom/sql/presto/ast/AstBuilder.h"

#include <any>
#include "velox/common/base/Exceptions.h"

namespace axiom::sql::presto {

namespace {
template <typename T>
bool isDistinct(T* context) {
  return context->setQuantifier() != nullptr &&
      context->setQuantifier()->DISTINCT() != nullptr;
}

std::optional<std::string> getText(antlr4::Token* token) {
  if (token == nullptr) {
    return std::nullopt;
  }
  return token->getText();
}

NodeLocation getLocation(antlr4::Token* token) {
  return NodeLocation(token->getLine(), token->getCharPositionInLine());
}

NodeLocation getLocation(antlr4::ParserRuleContext* ctx) {
  return getLocation(ctx->getStart());
}

NodeLocation getLocation(antlr4::tree::TerminalNode* terminalNode) {
  return getLocation(terminalNode->getSymbol());
}

// Remove leading and trailing quotes.
std::string unquote(const std::string& value) {
  return value.substr(1, value.length() - 2);
}

} // namespace

void AstBuilder::trace(const std::string& name) const {
  if (enableTracing_) {
    std::cout << name << std::endl;
  }
}

std::any AstBuilder::visitSingleStatement(
    PrestoSqlParser::SingleStatementContext* ctx) {
  return visit(ctx->statement());
}

std::any AstBuilder::visitQuery(PrestoSqlParser::QueryContext* ctx) {
  trace("visitQuery");

  auto queryNoWith = visitTyped<Query>(ctx->queryNoWith());

  // TODO: Handle with
  return std::static_pointer_cast<Statement>(std::make_shared<Query>(
      getLocation(ctx),
      queryNoWith->with(),
      queryNoWith->queryBody(),
      queryNoWith->orderBy(),
      queryNoWith->offset(),
      queryNoWith->limit()));
}

antlrcpp::Any AstBuilder::visitQueryNoWith(
    PrestoSqlParser::QueryNoWithContext* ctx) {
  trace("visitQueryNoWith");

  OrderByPtr orderBy = nullptr;
  if (ctx->ORDER() != nullptr) {
    orderBy = std::make_shared<OrderBy>(
        getLocation(ctx->ORDER()), visitAllContext<SortItem>(ctx->sortItem()));
  }

  OffsetPtr offset = nullptr;
  if (ctx->offset) {
    offset = std::make_shared<Offset>(getLocation(ctx), ctx->offset->getText());
  }

  auto limit = getText(ctx->limit);

  auto term = visit(ctx->queryTerm());
  try {
    auto querySpec = std::any_cast<std::shared_ptr<QuerySpecification>>(term);
    return std::make_shared<Query>(
        getLocation(ctx),
        /*with*/ nullptr,
        std::make_shared<QuerySpecification>(
            getLocation(ctx),
            querySpec->select(),
            querySpec->from(),
            querySpec->where(),
            querySpec->groupBy(),
            querySpec->having()),
        orderBy,
        offset,
        limit);
  } catch (const std::bad_any_cast& e) {
    throw std::runtime_error("Uninplemented for QueryNoWith");
  }
}

antlrcpp::Any AstBuilder::visitSelectSingle(
    PrestoSqlParser::SelectSingleContext* ctx) {
  trace("visitSelectSingle");
  auto expr = visitTyped<Expression>(ctx->expression());

  auto alias = visitTyped<Identifier>(ctx->identifier());

  return std::static_pointer_cast<SelectItem>(
      std::make_shared<SingleColumn>(getLocation(ctx), expr, alias));
}

antlrcpp::Any AstBuilder::visitQuerySpecification(
    PrestoSqlParser::QuerySpecificationContext* ctx) {
  trace("visitQuerySpecification");

  auto selectItems = visitAllContext<SelectItem>(ctx->selectItem());

  RelationPtr from;
  auto relations = visitAllContext<Relation>(ctx->relation());
  if (!relations.empty()) {
    // Synthesize implicit join nodes
    auto iterator = relations.begin();
    RelationPtr relation = *iterator;
    ++iterator;

    while (iterator != relations.end()) {
      relation = std::make_shared<Join>(
          getLocation(ctx),
          Join::Type::kImplicit,
          relation,
          *iterator,
          nullptr);
      ++iterator;
    }

    from = relation;
  }

  return std::make_shared<QuerySpecification>(
      getLocation(ctx),
      std::make_shared<Select>(
          getLocation(ctx), isDistinct(ctx), std::move(selectItems)),
      from,
      visitTyped<Expression>(ctx->where),
      visitTyped<GroupBy>(ctx->groupBy()),
      visitTyped<Expression>(ctx->having),
      nullptr // window
  );
}

antlrcpp::Any AstBuilder::visitSampledRelation(
    PrestoSqlParser::SampledRelationContext* ctx) {
  trace("visitSampledRelation");
  auto child = visit(ctx->aliasedRelation());
  if (!ctx->TABLESAMPLE()) {
    return child;
  }

  VELOX_NYI("TODO support visitSampledRelation for table sample");
}

antlrcpp::Any AstBuilder::visitAliasedRelation(
    PrestoSqlParser::AliasedRelationContext* ctx) {
  trace("visitAliasedRelation");
  auto child = visitTyped<Relation>(ctx->relationPrimary());
  if (!ctx->identifier()) {
    return child;
  }

  std::vector<IdentifierPtr> aliases;
  if (ctx->columnAliases() != nullptr) {
    aliases = visitAllContext<Identifier>(ctx->columnAliases()->identifier());
  }

  return std::static_pointer_cast<Relation>(std::make_shared<AliasedRelation>(
      getLocation(ctx), child, visitIdentifier(ctx->identifier()), aliases));
}

antlrcpp::Any AstBuilder::visitTableName(
    PrestoSqlParser::TableNameContext* ctx) {
  trace("visitTableName");

  auto name = getQualifiedName(ctx->qualifiedName());
  return std::static_pointer_cast<Relation>(
      std::make_shared<Table>(getLocation(ctx), name));
}

antlrcpp::Any AstBuilder::visitSelectAll(
    PrestoSqlParser::SelectAllContext* ctx) {
  trace("visitSelectAll");

  auto name = visitTyped<QualifiedName>(ctx->qualifiedName());

  return std::static_pointer_cast<SelectItem>(
      std::make_shared<AllColumns>(getLocation(ctx), name));
}

antlrcpp::Any AstBuilder::visitUnquotedIdentifier(
    PrestoSqlParser::UnquotedIdentifierContext* ctx) {
  return std::make_shared<Identifier>(getLocation(ctx), ctx->getText(), false);
}

// private
QualifiedNamePtr AstBuilder::getQualifiedName(
    PrestoSqlParser::QualifiedNameContext* ctx) {
  auto identifiers = visitAllContext<Identifier>(ctx->identifier());

  std::vector<std::string> names;
  names.reserve(identifiers.size());
  for (auto& identifier : identifiers) {
    names.push_back(identifier->value());
  }
  return std::make_shared<QualifiedName>(getLocation(ctx), std::move(names));
}

antlrcpp::Any AstBuilder::visitStandaloneExpression(
    PrestoSqlParser::StandaloneExpressionContext* ctx) {
  trace("visitStandaloneExpression");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitStandaloneRoutineBody(
    PrestoSqlParser::StandaloneRoutineBodyContext* ctx) {
  trace("visitStandaloneRoutineBody");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitStatementDefault(
    PrestoSqlParser::StatementDefaultContext* ctx) {
  trace("visitStatementDefault");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitUse(PrestoSqlParser::UseContext* ctx) {
  trace("visitUse");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCreateSchema(
    PrestoSqlParser::CreateSchemaContext* ctx) {
  trace("visitCreateSchema");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDropSchema(
    PrestoSqlParser::DropSchemaContext* ctx) {
  trace("visitDropSchema");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRenameSchema(
    PrestoSqlParser::RenameSchemaContext* ctx) {
  trace("visitRenameSchema");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCreateTableAsSelect(
    PrestoSqlParser::CreateTableAsSelectContext* ctx) {
  trace("visitCreateTableAsSelect");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCreateTable(
    PrestoSqlParser::CreateTableContext* ctx) {
  trace("visitCreateTable");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDropTable(
    PrestoSqlParser::DropTableContext* ctx) {
  trace("visitDropTable");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitInsertInto(
    PrestoSqlParser::InsertIntoContext* ctx) {
  trace("visitInsertInto");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDelete(PrestoSqlParser::DeleteContext* ctx) {
  trace("visitDelete");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitTruncateTable(
    PrestoSqlParser::TruncateTableContext* ctx) {
  trace("visitTruncateTable");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRenameTable(
    PrestoSqlParser::RenameTableContext* ctx) {
  trace("visitRenameTable");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRenameColumn(
    PrestoSqlParser::RenameColumnContext* ctx) {
  trace("visitRenameColumn");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDropColumn(
    PrestoSqlParser::DropColumnContext* ctx) {
  trace("visitDropColumn");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitAddColumn(
    PrestoSqlParser::AddColumnContext* ctx) {
  trace("visitAddColumn");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitAddConstraint(
    PrestoSqlParser::AddConstraintContext* ctx) {
  trace("visitAddConstraint");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDropConstraint(
    PrestoSqlParser::DropConstraintContext* ctx) {
  trace("visitDropConstraint");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitAlterColumnSetNotNull(
    PrestoSqlParser::AlterColumnSetNotNullContext* ctx) {
  trace("visitAlterColumnSetNotNull");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitAlterColumnDropNotNull(
    PrestoSqlParser::AlterColumnDropNotNullContext* ctx) {
  trace("visitAlterColumnDropNotNull");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSetTableProperties(
    PrestoSqlParser::SetTablePropertiesContext* ctx) {
  trace("visitSetTableProperties");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitAnalyze(PrestoSqlParser::AnalyzeContext* ctx) {
  trace("visitAnalyze");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCreateType(
    PrestoSqlParser::CreateTypeContext* ctx) {
  trace("visitCreateType");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCreateView(
    PrestoSqlParser::CreateViewContext* ctx) {
  trace("visitCreateView");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRenameView(
    PrestoSqlParser::RenameViewContext* ctx) {
  trace("visitRenameView");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDropView(PrestoSqlParser::DropViewContext* ctx) {
  trace("visitDropView");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCreateMaterializedView(
    PrestoSqlParser::CreateMaterializedViewContext* ctx) {
  trace("visitCreateMaterializedView");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDropMaterializedView(
    PrestoSqlParser::DropMaterializedViewContext* ctx) {
  trace("visitDropMaterializedView");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRefreshMaterializedView(
    PrestoSqlParser::RefreshMaterializedViewContext* ctx) {
  trace("visitRefreshMaterializedView");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCreateFunction(
    PrestoSqlParser::CreateFunctionContext* ctx) {
  trace("visitCreateFunction");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitAlterFunction(
    PrestoSqlParser::AlterFunctionContext* ctx) {
  trace("visitAlterFunction");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDropFunction(
    PrestoSqlParser::DropFunctionContext* ctx) {
  trace("visitDropFunction");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCall(PrestoSqlParser::CallContext* ctx) {
  trace("visitCall");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCreateRole(
    PrestoSqlParser::CreateRoleContext* ctx) {
  trace("visitCreateRole");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDropRole(PrestoSqlParser::DropRoleContext* ctx) {
  trace("visitDropRole");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitGrantRoles(
    PrestoSqlParser::GrantRolesContext* ctx) {
  trace("visitGrantRoles");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRevokeRoles(
    PrestoSqlParser::RevokeRolesContext* ctx) {
  trace("visitRevokeRoles");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSetRole(PrestoSqlParser::SetRoleContext* ctx) {
  trace("visitSetRole");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitGrant(PrestoSqlParser::GrantContext* ctx) {
  trace("visitGrant");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRevoke(PrestoSqlParser::RevokeContext* ctx) {
  trace("visitRevoke");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowGrants(
    PrestoSqlParser::ShowGrantsContext* ctx) {
  trace("visitShowGrants");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitExplain(PrestoSqlParser::ExplainContext* ctx) {
  trace("visitExplain");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowCreateTable(
    PrestoSqlParser::ShowCreateTableContext* ctx) {
  trace("visitShowCreateTable");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowCreateView(
    PrestoSqlParser::ShowCreateViewContext* ctx) {
  trace("visitShowCreateView");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowCreateMaterializedView(
    PrestoSqlParser::ShowCreateMaterializedViewContext* ctx) {
  trace("visitShowCreateMaterializedView");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowCreateFunction(
    PrestoSqlParser::ShowCreateFunctionContext* ctx) {
  trace("visitShowCreateFunction");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowTables(
    PrestoSqlParser::ShowTablesContext* ctx) {
  trace("visitShowTables");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowSchemas(
    PrestoSqlParser::ShowSchemasContext* ctx) {
  trace("visitShowSchemas");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowCatalogs(
    PrestoSqlParser::ShowCatalogsContext* ctx) {
  trace("visitShowCatalogs");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowColumns(
    PrestoSqlParser::ShowColumnsContext* ctx) {
  trace("visitShowColumns");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowStats(
    PrestoSqlParser::ShowStatsContext* ctx) {
  trace("visitShowStats");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowStatsForQuery(
    PrestoSqlParser::ShowStatsForQueryContext* ctx) {
  trace("visitShowStatsForQuery");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowRoles(
    PrestoSqlParser::ShowRolesContext* ctx) {
  trace("visitShowRoles");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowRoleGrants(
    PrestoSqlParser::ShowRoleGrantsContext* ctx) {
  trace("visitShowRoleGrants");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowFunctions(
    PrestoSqlParser::ShowFunctionsContext* ctx) {
  trace("visitShowFunctions");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitShowSession(
    PrestoSqlParser::ShowSessionContext* ctx) {
  trace("visitShowSession");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSetSession(
    PrestoSqlParser::SetSessionContext* ctx) {
  trace("visitSetSession");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitResetSession(
    PrestoSqlParser::ResetSessionContext* ctx) {
  trace("visitResetSession");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitStartTransaction(
    PrestoSqlParser::StartTransactionContext* ctx) {
  trace("visitStartTransaction");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCommit(PrestoSqlParser::CommitContext* ctx) {
  trace("visitCommit");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRollback(PrestoSqlParser::RollbackContext* ctx) {
  trace("visitRollback");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitPrepare(PrestoSqlParser::PrepareContext* ctx) {
  trace("visitPrepare");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDeallocate(
    PrestoSqlParser::DeallocateContext* ctx) {
  trace("visitDeallocate");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitExecute(PrestoSqlParser::ExecuteContext* ctx) {
  trace("visitExecute");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDescribeInput(
    PrestoSqlParser::DescribeInputContext* ctx) {
  trace("visitDescribeInput");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDescribeOutput(
    PrestoSqlParser::DescribeOutputContext* ctx) {
  trace("visitDescribeOutput");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitUpdate(PrestoSqlParser::UpdateContext* ctx) {
  trace("visitUpdate");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitWith(PrestoSqlParser::WithContext* ctx) {
  trace("visitWith");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitTableElement(
    PrestoSqlParser::TableElementContext* ctx) {
  trace("visitTableElement");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitColumnDefinition(
    PrestoSqlParser::ColumnDefinitionContext* ctx) {
  trace("visitColumnDefinition");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitLikeClause(
    PrestoSqlParser::LikeClauseContext* ctx) {
  trace("visitLikeClause");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitProperties(
    PrestoSqlParser::PropertiesContext* ctx) {
  trace("visitProperties");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitProperty(PrestoSqlParser::PropertyContext* ctx) {
  trace("visitProperty");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSqlParameterDeclaration(
    PrestoSqlParser::SqlParameterDeclarationContext* ctx) {
  trace("visitSqlParameterDeclaration");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRoutineCharacteristics(
    PrestoSqlParser::RoutineCharacteristicsContext* ctx) {
  trace("visitRoutineCharacteristics");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRoutineCharacteristic(
    PrestoSqlParser::RoutineCharacteristicContext* ctx) {
  trace("visitRoutineCharacteristic");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitAlterRoutineCharacteristics(
    PrestoSqlParser::AlterRoutineCharacteristicsContext* ctx) {
  trace("visitAlterRoutineCharacteristics");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitAlterRoutineCharacteristic(
    PrestoSqlParser::AlterRoutineCharacteristicContext* ctx) {
  trace("visitAlterRoutineCharacteristic");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRoutineBody(
    PrestoSqlParser::RoutineBodyContext* ctx) {
  trace("visitRoutineBody");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitReturnStatement(
    PrestoSqlParser::ReturnStatementContext* ctx) {
  trace("visitReturnStatement");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitExternalBodyReference(
    PrestoSqlParser::ExternalBodyReferenceContext* ctx) {
  trace("visitExternalBodyReference");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitLanguage(PrestoSqlParser::LanguageContext* ctx) {
  trace("visitLanguage");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDeterminism(
    PrestoSqlParser::DeterminismContext* ctx) {
  trace("visitDeterminism");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitNullCallClause(
    PrestoSqlParser::NullCallClauseContext* ctx) {
  trace("visitNullCallClause");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitExternalRoutineName(
    PrestoSqlParser::ExternalRoutineNameContext* ctx) {
  trace("visitExternalRoutineName");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitQueryTermDefault(
    PrestoSqlParser::QueryTermDefaultContext* ctx) {
  trace("visitQueryTermDefault");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSetOperation(
    PrestoSqlParser::SetOperationContext* ctx) {
  trace("visitSetOperation");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitQueryPrimaryDefault(
    PrestoSqlParser::QueryPrimaryDefaultContext* ctx) {
  trace("visitQueryPrimaryDefault");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitTable(PrestoSqlParser::TableContext* ctx) {
  trace("visitTable");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitInlineTable(
    PrestoSqlParser::InlineTableContext* ctx) {
  trace("visitInlineTable");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSubquery(PrestoSqlParser::SubqueryContext* ctx) {
  trace("visitSubquery");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSortItem(PrestoSqlParser::SortItemContext* ctx) {
  trace("visitSortItem");

  auto expression = visitTyped<Expression>(ctx->expression());

  SortItem::Ordering ordering = SortItem::Ordering::kAscending;
  if (ctx->ordering) {
    auto tokenType = ctx->ordering->getType();
    if (tokenType == PrestoSqlParser::ASC) {
      ordering = SortItem::Ordering::kAscending;
    } else if (tokenType == PrestoSqlParser::DESC) {
      ordering = SortItem::Ordering::kDescending;
    }
  }

  SortItem::NullOrdering nullOrdering = SortItem::NullOrdering::kUndefined;
  if (ctx->nullOrdering) {
    auto tokenType = ctx->nullOrdering->getType();
    if (tokenType == PrestoSqlParser::FIRST) {
      nullOrdering = SortItem::NullOrdering::kFirst;
    } else if (tokenType == PrestoSqlParser::LAST) {
      nullOrdering = SortItem::NullOrdering::kLast;
    }
  }

  return std::make_shared<SortItem>(
      getLocation(ctx), expression, ordering, nullOrdering);
}

antlrcpp::Any AstBuilder::visitGroupBy(PrestoSqlParser::GroupByContext* ctx) {
  trace("visitGroupBy");

  auto groupingElements =
      visitAllContext<GroupingElement>(ctx->groupingElement());

  return std::make_shared<GroupBy>(
      getLocation(ctx), isDistinct(ctx), groupingElements);
}

antlrcpp::Any AstBuilder::visitSingleGroupingSet(
    PrestoSqlParser::SingleGroupingSetContext* ctx) {
  trace("visitSingleGroupingSet");

  auto expressions =
      visitAllContext<Expression>(ctx->groupingSet()->expression());
  return std::static_pointer_cast<GroupingElement>(
      std::make_shared<SimpleGroupBy>(getLocation(ctx), expressions));
}

antlrcpp::Any AstBuilder::visitRollup(PrestoSqlParser::RollupContext* ctx) {
  trace("visitRollup");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCube(PrestoSqlParser::CubeContext* ctx) {
  trace("visitCube");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitMultipleGroupingSets(
    PrestoSqlParser::MultipleGroupingSetsContext* ctx) {
  trace("visitMultipleGroupingSets");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitGroupingSet(
    PrestoSqlParser::GroupingSetContext* ctx) {
  trace("visitGroupingSet");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitNamedQuery(
    PrestoSqlParser::NamedQueryContext* ctx) {
  trace("visitNamedQuery");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSetQuantifier(
    PrestoSqlParser::SetQuantifierContext* ctx) {
  trace("visitSetQuantifier");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRelationDefault(
    PrestoSqlParser::RelationDefaultContext* ctx) {
  trace("visitRelationDefault");
  return visitChildren(ctx);
}

namespace {
Join::Type toJoinType(PrestoSqlParser::JoinTypeContext* joinTypeCtx) {
  if (!joinTypeCtx) {
    return Join::Type::kInner;
  }

  if (joinTypeCtx->LEFT() != nullptr) {
    return Join::Type::kLeft;
  } else if (joinTypeCtx->RIGHT() != nullptr) {
    return Join::Type::kRight;
  } else if (joinTypeCtx->FULL() != nullptr) {
    return Join::Type::kFull;
  }

  return Join::Type::kInner;
}

} // anonymous namespace

antlrcpp::Any AstBuilder::visitJoinRelation(
    PrestoSqlParser::JoinRelationContext* ctx) {
  trace("visitJoinRelation");

  auto left = visitTyped<Relation>(ctx->left);

  if (ctx->CROSS() != nullptr) {
    auto right = visitTyped<Relation>(ctx->right);
    return std::static_pointer_cast<Relation>(std::make_shared<Join>(
        getLocation(ctx), Join::Type::kCross, left, right, nullptr));
  }

  if (ctx->NATURAL() != nullptr) {
    auto right = visitTyped<Relation>(ctx->right);
    auto joinType = toJoinType(ctx->joinType());
    return std::static_pointer_cast<Relation>(
        std::make_shared<NaturalJoin>(getLocation(ctx), joinType, left, right));
  }

  // Handle regular join with criteria.
  auto right = visitTyped<Relation>(ctx->rightRelation);

  JoinCriteriaPtr joinCriteria;
  if (auto criteria = ctx->joinCriteria()) {
    if (criteria->ON() != nullptr) {
      auto expression = visitExpression(criteria->booleanExpression());
      joinCriteria = std::make_shared<JoinOn>(
          getLocation(ctx->joinCriteria()), expression);
    } else if (criteria->USING() != nullptr) {
      std::vector<IdentifierPtr> columns;
      for (auto identifierCtx : criteria->identifier()) {
        auto identifier = visitIdentifier(identifierCtx);
        columns.push_back(identifier);
      }
      joinCriteria = std::make_shared<JoinUsing>(
          getLocation(ctx->joinCriteria()), columns);
    } else {
      throw std::runtime_error("Unsupported join criteria");
    }
  }

  auto joinType = toJoinType(ctx->joinType());

  return std::static_pointer_cast<Relation>(std::make_shared<Join>(
      getLocation(ctx), joinType, left, right, joinCriteria));
}

antlrcpp::Any AstBuilder::visitJoinType(PrestoSqlParser::JoinTypeContext* ctx) {
  trace("visitJoinType");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitJoinCriteria(
    PrestoSqlParser::JoinCriteriaContext* ctx) {
  trace("visitJoinCriteria");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSampleType(
    PrestoSqlParser::SampleTypeContext* ctx) {
  trace("visitSampleType");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitColumnAliases(
    PrestoSqlParser::ColumnAliasesContext* ctx) {
  trace("visitColumnAliases");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSubqueryRelation(
    PrestoSqlParser::SubqueryRelationContext* ctx) {
  trace("visitSubqueryRelation");

  return std::static_pointer_cast<Relation>(std::make_shared<TableSubquery>(
      getLocation(ctx), visitTyped<Statement>(ctx->query())));
}

antlrcpp::Any AstBuilder::visitUnnest(PrestoSqlParser::UnnestContext* ctx) {
  trace("visitUnnest");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitLateral(PrestoSqlParser::LateralContext* ctx) {
  trace("visitLateral");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitParenthesizedRelation(
    PrestoSqlParser::ParenthesizedRelationContext* ctx) {
  trace("visitParenthesizedRelation");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitExpression(
    PrestoSqlParser::ExpressionContext* ctx) {
  trace("visitExpression");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitLogicalNot(
    PrestoSqlParser::LogicalNotContext* ctx) {
  trace("visitLogicalNot");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitPredicated(
    PrestoSqlParser::PredicatedContext* ctx) {
  trace("visitPredicated");

  if (ctx->predicate() != nullptr) {
    return visitExpression(ctx->predicate());
  }

  return visitExpression(ctx->valueExpression());
}

antlrcpp::Any AstBuilder::visitLogicalBinary(
    PrestoSqlParser::LogicalBinaryContext* ctx) {
  trace("visitLogicalBinary");

  auto leftExpr = visitExpression(ctx->left);
  auto rightExpr = visitExpression(ctx->right);

  LogicalBinaryExpression::Operator op = ctx->AND() != nullptr
      ? LogicalBinaryExpression::Operator::kAnd
      : LogicalBinaryExpression::Operator::kOr;

  return std::static_pointer_cast<Expression>(
      std::make_shared<LogicalBinaryExpression>(
          getLocation(ctx), op, leftExpr, rightExpr));
}

namespace {

ComparisonExpression::Operator toComparisonOperator(size_t tokenType) {
  switch (tokenType) {
    case PrestoSqlParser::EQ:
      return ComparisonExpression::Operator::kEqual;
    case PrestoSqlParser::NEQ:
      return ComparisonExpression::Operator::kNotEqual;
    case PrestoSqlParser::LT:
      return ComparisonExpression::Operator::kLessThan;
    case PrestoSqlParser::LTE:
      return ComparisonExpression::Operator::kLessThanOrEqual;
    case PrestoSqlParser::GT:
      return ComparisonExpression::Operator::kGreaterThan;
    case PrestoSqlParser::GTE:
      return ComparisonExpression::Operator::kGreaterThanOrEqual;
    default:
      throw std::runtime_error(
          "Unsupported comparison operator: " + std::to_string(tokenType));
  }
}

} // anonymous namespace

antlrcpp::Any AstBuilder::visitComparison(
    PrestoSqlParser::ComparisonContext* ctx) {
  trace("visitComparison");

  auto leftExpr = visitExpression(ctx->value);
  auto rightExpr = visitExpression(ctx->right);

  auto operatorToken = ctx->comparisonOperator()->children[0];
  auto terminalNode = dynamic_cast<antlr4::tree::TerminalNode*>(operatorToken);
  auto op = toComparisonOperator(terminalNode->getSymbol()->getType());

  return std::static_pointer_cast<Expression>(
      std::make_shared<ComparisonExpression>(
          getLocation(ctx), op, leftExpr, rightExpr));
}

antlrcpp::Any AstBuilder::visitQuantifiedComparison(
    PrestoSqlParser::QuantifiedComparisonContext* ctx) {
  trace("visitQuantifiedComparison");
  return visitChildren(ctx);
}

namespace {
ExpressionPtr wrapInNot(
    const ExpressionPtr& expr,
    antlr4::tree::TerminalNode* notNode) {
  if (notNode != nullptr) {
    return std::static_pointer_cast<Expression>(
        std::make_shared<NotExpression>(expr->location(), expr));
  }

  return expr;
}
} // namespace

antlrcpp::Any AstBuilder::visitBetween(PrestoSqlParser::BetweenContext* ctx) {
  trace("visitBetween");

  auto between = std::make_shared<BetweenPredicate>(
      getLocation(ctx),
      visitExpression(ctx->value),
      visitExpression(ctx->lower),
      visitExpression(ctx->upper));

  return wrapInNot(between, ctx->NOT());
}

antlrcpp::Any AstBuilder::visitInList(PrestoSqlParser::InListContext* ctx) {
  trace("visitInList");

  auto inPredicate = std::make_shared<InPredicate>(
      getLocation(ctx),
      visitTyped<Expression>(ctx->value),
      std::make_shared<InListExpression>(
          getLocation(ctx), visitAllContext<Expression>(ctx->expression())));

  return wrapInNot(inPredicate, ctx->NOT());
}

antlrcpp::Any AstBuilder::visitInSubquery(
    PrestoSqlParser::InSubqueryContext* ctx) {
  trace("visitInSubquery");

  auto inPredicate =
      std::static_pointer_cast<Expression>(std::make_shared<InPredicate>(
          getLocation(ctx),
          visitTyped<Expression>(ctx->value),
          std::make_shared<SubqueryExpression>(
              getLocation(ctx), visitTyped<Statement>(ctx->query()))));

  return wrapInNot(inPredicate, ctx->NOT());
}

antlrcpp::Any AstBuilder::visitLike(PrestoSqlParser::LikeContext* ctx) {
  trace("visitLike");

  auto like = std::make_shared<LikePredicate>(
      getLocation(ctx),
      visitTyped<Expression>(ctx->value),
      visitTyped<Expression>(ctx->pattern),
      visitTyped<Expression>(ctx->escape));

  return wrapInNot(like, ctx->NOT());
}

antlrcpp::Any AstBuilder::visitNullPredicate(
    PrestoSqlParser::NullPredicateContext* ctx) {
  trace("visitNullPredicate");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDistinctFrom(
    PrestoSqlParser::DistinctFromContext* ctx) {
  trace("visitDistinctFrom");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitValueExpressionDefault(
    PrestoSqlParser::ValueExpressionDefaultContext* ctx) {
  trace("visitValueExpressionDefault");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitConcatenation(
    PrestoSqlParser::ConcatenationContext* ctx) {
  trace("visitConcatenation");
  return visitChildren(ctx);
}

namespace {
ArithmeticBinaryExpression::Operator toArithmeticBinaryOperator(
    size_t tokenType) {
  switch (tokenType) {
    case PrestoSqlParser::PLUS:
      return ArithmeticBinaryExpression::Operator::kAdd;
    case PrestoSqlParser::MINUS:
      return ArithmeticBinaryExpression::Operator::kSubtract;
    case PrestoSqlParser::ASTERISK:
      return ArithmeticBinaryExpression::Operator::kMultiply;
    case PrestoSqlParser::SLASH:
      return ArithmeticBinaryExpression::Operator::kDivide;
    case PrestoSqlParser::PERCENT:
      return ArithmeticBinaryExpression::Operator::kModulus;
    default:
      throw std::runtime_error(
          "Unsupported arithmetic operator: " + std::to_string(tokenType));
  }
}

} // anonymous namespace

antlrcpp::Any AstBuilder::visitArithmeticBinary(
    PrestoSqlParser::ArithmeticBinaryContext* ctx) {
  trace("visitArithmeticBinary");

  auto leftExpr = visitExpression(ctx->left);
  auto rightExpr = visitExpression(ctx->right);

  auto op = toArithmeticBinaryOperator(ctx->op->getType());

  return std::static_pointer_cast<Expression>(
      std::make_shared<ArithmeticBinaryExpression>(
          getLocation(ctx), op, leftExpr, rightExpr));
}

antlrcpp::Any AstBuilder::visitArithmeticUnary(
    PrestoSqlParser::ArithmeticUnaryContext* ctx) {
  trace("visitArithmeticUnary");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitAtTimeZone(
    PrestoSqlParser::AtTimeZoneContext* ctx) {
  trace("visitAtTimeZone");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDereference(
    PrestoSqlParser::DereferenceContext* ctx) {
  trace("visitDereference");

  return std::static_pointer_cast<Expression>(
      std::make_shared<DereferenceExpression>(
          getLocation(ctx),
          visitExpression(ctx->base),
          visitIdentifier(ctx->fieldName)));
}

namespace {

std::string getIntervalFieldType(
    PrestoSqlParser::IntervalFieldContext* intervalField) {
  if (intervalField->YEAR() != nullptr) {
    return "YEAR";
  } else if (intervalField->MONTH() != nullptr) {
    return "MONTH";
  } else if (intervalField->DAY() != nullptr) {
    return "DAY";
  } else if (intervalField->HOUR() != nullptr) {
    return "HOUR";
  } else if (intervalField->MINUTE() != nullptr) {
    return "MINUTE";
  } else if (intervalField->SECOND() != nullptr) {
    return "SECOND";
  } else {
    throw std::runtime_error(
        "Unsupported interval field: " + intervalField->getText());
  }
}

TypeSignaturePtr toTypeSignature(
    PrestoSqlParser::TypeParameterContext* typeParam,
    const std::optional<std::string>& rowFieldName = std::nullopt);

TypeSignaturePtr toTypeSignature(
    PrestoSqlParser::TypeContext* ctx,
    const std::optional<std::string>& rowFieldName = std::nullopt) {
  if (ctx->baseType() != nullptr) {
    if (ctx->baseType()->DOUBLE_PRECISION() != nullptr) {
      return std::make_shared<TypeSignature>(
          getLocation(ctx), "DOUBLE", rowFieldName);
    }

    auto baseName = ctx->baseType()->getText();

    std::vector<TypeSignaturePtr> parameters;
    for (const auto& param : ctx->typeParameter()) {
      parameters.push_back(toTypeSignature(param));
    }

    return std::make_shared<TypeSignature>(
        getLocation(ctx),
        std::move(baseName),
        std::move(parameters),
        rowFieldName);
  }

  if (ctx->ARRAY() != nullptr) {
    return std::make_shared<TypeSignature>(
        getLocation(ctx),
        "ARRAY",
        std::vector<TypeSignaturePtr>{toTypeSignature(ctx->type(0))},
        rowFieldName);
  }

  if (ctx->MAP() != nullptr) {
    return std::make_shared<TypeSignature>(
        getLocation(ctx),
        "MAP",
        std::vector<TypeSignaturePtr>{
            toTypeSignature(ctx->type(0)), toTypeSignature(ctx->type(1))},
        rowFieldName);
  }

  if (ctx->ROW() != nullptr) {
    const auto& identifiers = ctx->identifier();
    const auto& typeParams = ctx->type();

    std::vector<TypeSignaturePtr> parameters;
    parameters.reserve(typeParams.size());
    for (auto i = 0; i < typeParams.size(); ++i) {
      parameters.push_back(
          toTypeSignature(typeParams[i], identifiers[i]->getText()));
    }

    return std::make_shared<TypeSignature>(
        getLocation(ctx), "ROW", std::move(parameters), rowFieldName);
  }

  if (ctx->INTERVAL() != nullptr) {
    const auto& intervalFields = ctx->intervalField();
    if (intervalFields.size() >= 2) {
      return std::make_shared<TypeSignature>(
          getLocation(ctx),
          "INTERVAL " + getIntervalFieldType(intervalFields[0]) + " TO " +
              getIntervalFieldType(intervalFields[1]),
          rowFieldName);
    }
  }

  throw std::runtime_error("Unsupported type specification: " + ctx->getText());
}

TypeSignaturePtr toTypeSignature(
    PrestoSqlParser::TypeParameterContext* ctx,
    const std::optional<std::string>& rowFieldName) {
  if (ctx->INTEGER_VALUE() != nullptr) {
    return std::make_shared<TypeSignature>(
        getLocation(ctx), ctx->INTEGER_VALUE()->getText(), rowFieldName);
  }

  if (ctx->type() != nullptr) {
    return toTypeSignature(ctx->type(), rowFieldName);
  }

  throw std::runtime_error("Unsupported typeParameter: " + ctx->getText());
}

bool equalsIgnoreCase(std::string_view left, std::string_view right) {
  if (left.size() != right.size()) {
    return false;
  }

  const auto n = left.size();
  for (auto i = 0; i < n; ++i) {
    if (std::toupper(left[i]) != std::toupper(right[i])) {
      return false;
    }
  }

  return true;
}

} // namespace

antlrcpp::Any AstBuilder::visitTypeConstructor(
    PrestoSqlParser::TypeConstructorContext* ctx) {
  trace("visitTypeConstructor");

  auto value = visitExpression(ctx->string())->as<StringLiteral>()->value();

  if (ctx->DOUBLE_PRECISION() != nullptr) {
    return std::static_pointer_cast<Expression>(
        std::make_shared<GenericLiteral>(
            getLocation(ctx),
            std::make_shared<TypeSignature>(getLocation(ctx), "DOUBLE"),
            value));
  }

  const auto type = toTypeSignature(ctx->type());
  const auto& baseName = type->baseName();

  if (equalsIgnoreCase(baseName, "time")) {
    return std::static_pointer_cast<Expression>(
        std::make_shared<TimeLiteral>(getLocation(ctx), value));
  }

  if (equalsIgnoreCase(baseName, "timestamp")) {
    return std::static_pointer_cast<Expression>(
        std::make_shared<TimestampLiteral>(getLocation(ctx), value));
  }

  if (equalsIgnoreCase(baseName, "decimal")) {
    return std::static_pointer_cast<Expression>(
        std::make_shared<DecimalLiteral>(getLocation(ctx), value));
  }

  if (equalsIgnoreCase(baseName, "char")) {
    return std::static_pointer_cast<Expression>(
        std::make_shared<CharLiteral>(getLocation(ctx), value));
  }

  return std::static_pointer_cast<Expression>(
      std::make_shared<GenericLiteral>(getLocation(ctx), type, value));
}

antlrcpp::Any AstBuilder::visitSpecialDateTimeFunction(
    PrestoSqlParser::SpecialDateTimeFunctionContext* ctx) {
  trace("visitSpecialDateTimeFunction");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSubstring(
    PrestoSqlParser::SubstringContext* ctx) {
  trace("visitSubstring");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCast(PrestoSqlParser::CastContext* ctx) {
  trace("visitCast");

  const bool isTryCast = ctx->TRY_CAST() != nullptr;

  return std::static_pointer_cast<Expression>(std::make_shared<Cast>(
      getLocation(ctx),
      visitTyped<Expression>(ctx->expression()),
      toTypeSignature(ctx->type()),
      isTryCast));
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitLambda(PrestoSqlParser::LambdaContext* ctx) {
  trace("visitLambda");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitParenthesizedExpression(
    PrestoSqlParser::ParenthesizedExpressionContext* ctx) {
  trace("visitParenthesizedExpression");
  return visit(ctx->expression());
}

antlrcpp::Any AstBuilder::visitParameter(
    PrestoSqlParser::ParameterContext* ctx) {
  trace("visitParameter");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitNormalize(
    PrestoSqlParser::NormalizeContext* ctx) {
  trace("visitNormalize");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitIntervalLiteral(
    PrestoSqlParser::IntervalLiteralContext* ctx) {
  trace("visitIntervalLiteral");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitNumericLiteral(
    PrestoSqlParser::NumericLiteralContext* ctx) {
  trace("visitNumericLiteral");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitBooleanLiteral(
    PrestoSqlParser::BooleanLiteralContext* ctx) {
  trace("visitBooleanLiteral");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSimpleCase(
    PrestoSqlParser::SimpleCaseContext* ctx) {
  trace("visitSimpleCase");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitColumnReference(
    PrestoSqlParser::ColumnReferenceContext* ctx) {
  trace("visitColumnReference");
  return std::static_pointer_cast<Expression>(
      visitIdentifier(ctx->identifier()));
}

antlrcpp::Any AstBuilder::visitNullLiteral(
    PrestoSqlParser::NullLiteralContext* ctx) {
  trace("visitNullLiteral");

  return std::static_pointer_cast<Expression>(
      std::make_shared<NullLiteral>(getLocation(ctx)));
}

antlrcpp::Any AstBuilder::visitRowConstructor(
    PrestoSqlParser::RowConstructorContext* ctx) {
  trace("visitRowConstructor");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSubscript(
    PrestoSqlParser::SubscriptContext* ctx) {
  trace("visitSubscript");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSubqueryExpression(
    PrestoSqlParser::SubqueryExpressionContext* ctx) {
  trace("visitSubqueryExpression");

  return std::static_pointer_cast<Expression>(
      std::make_shared<SubqueryExpression>(
          getLocation(ctx), visitTyped<Statement>(ctx->query())));
}

antlrcpp::Any AstBuilder::visitBinaryLiteral(
    PrestoSqlParser::BinaryLiteralContext* ctx) {
  trace("visitBinaryLiteral");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCurrentUser(
    PrestoSqlParser::CurrentUserContext* ctx) {
  trace("visitCurrentUser");
  return visitChildren(ctx);
}

namespace {
Extract::Field toField(const std::string& name) {
  if (equalsIgnoreCase(name, "YEAR")) {
    return Extract::Field::kYear;
  }

  if (equalsIgnoreCase(name, "QUARTER")) {
    return Extract::Field::kQuarter;
  }

  if (equalsIgnoreCase(name, "MONTH")) {
    return Extract::Field::kMonth;
  }

  if (equalsIgnoreCase(name, "WEEK")) {
    return Extract::Field::kWeek;
  }

  if (equalsIgnoreCase(name, "DAY")) {
    return Extract::Field::kDay;
  }

  if (equalsIgnoreCase(name, "DAY_OF_MONTH")) {
    return Extract::Field::kDayOfMonth;
  }

  if (equalsIgnoreCase(name, "DAY_OF_WEEK")) {
    return Extract::Field::kDayOfWeek;
  }

  if (equalsIgnoreCase(name, "DOW")) {
    return Extract::Field::kDow;
  }

  if (equalsIgnoreCase(name, "DAY_OF_YEAR")) {
    return Extract::Field::kDayOfYear;
  }

  if (equalsIgnoreCase(name, "DOY")) {
    return Extract::Field::kDoy;
  }

  if (equalsIgnoreCase(name, "YEAR_OF_WEEK")) {
    return Extract::Field::kYearOfWeek;
  }

  if (equalsIgnoreCase(name, "YOW")) {
    return Extract::Field::kYow;
  }

  if (equalsIgnoreCase(name, "HOUR")) {
    return Extract::Field::kHour;
  }

  if (equalsIgnoreCase(name, "MINUTE")) {
    return Extract::Field::kMinute;
  }

  if (equalsIgnoreCase(name, "SECOND")) {
    return Extract::Field::kSecond;
  }

  if (equalsIgnoreCase(name, "TIMEZONE_HOUR")) {
    return Extract::Field::kTimezoneHour;
  }

  if (equalsIgnoreCase(name, "TIMEZONE_MINUTE")) {
    return Extract::Field::kTimezoneMinute;
  }

  throw std::runtime_error(fmt::format("Invalid EXTRACT field: {}", name));
}
} // namespace

antlrcpp::Any AstBuilder::visitExtract(PrestoSqlParser::ExtractContext* ctx) {
  trace("visitExtract");

  const auto field = visitIdentifier(ctx->identifier())->value();

  return std::static_pointer_cast<Expression>(std::make_shared<Extract>(
      getLocation(ctx),
      visitExpression(ctx->valueExpression()),
      toField(field)));
}

antlrcpp::Any AstBuilder::visitStringLiteral(
    PrestoSqlParser::StringLiteralContext* ctx) {
  trace("visitStringLiteral");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitArrayConstructor(
    PrestoSqlParser::ArrayConstructorContext* ctx) {
  trace("visitArrayConstructor");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitFunctionCall(
    PrestoSqlParser::FunctionCallContext* ctx) {
  trace("visitFunctionCall");

  auto name = getQualifiedName(ctx->qualifiedName());

  auto args = visitAllContext<Expression>(ctx->expression());

  return std::static_pointer_cast<Expression>(std::make_shared<FunctionCall>(
      getLocation(ctx), name, nullptr /* window */, isDistinct(ctx), args));
}

antlrcpp::Any AstBuilder::visitExists(PrestoSqlParser::ExistsContext* ctx) {
  trace("visitExists");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitPosition(PrestoSqlParser::PositionContext* ctx) {
  trace("visitPosition");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSearchedCase(
    PrestoSqlParser::SearchedCaseContext* ctx) {
  trace("visitSearchedCase");

  return std::static_pointer_cast<Expression>(
      std::make_shared<SearchedCaseExpression>(
          getLocation(ctx),
          visitAllContext<WhenClause>(ctx->whenClause()),
          visitTyped<Expression>(ctx->elseExpression)));
}

antlrcpp::Any AstBuilder::visitGroupingOperation(
    PrestoSqlParser::GroupingOperationContext* ctx) {
  trace("visitGroupingOperation");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitBasicStringLiteral(
    PrestoSqlParser::BasicStringLiteralContext* ctx) {
  trace("visitBasicStringLiteral");
  return std::static_pointer_cast<Expression>(std::make_shared<StringLiteral>(
      getLocation(ctx), unquote(ctx->STRING()->getText())));
}

antlrcpp::Any AstBuilder::visitUnicodeStringLiteral(
    PrestoSqlParser::UnicodeStringLiteralContext* ctx) {
  trace("visitUnicodeStringLiteral");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitNullTreatment(
    PrestoSqlParser::NullTreatmentContext* ctx) {
  trace("visitNullTreatment");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitTimeZoneInterval(
    PrestoSqlParser::TimeZoneIntervalContext* ctx) {
  trace("visitTimeZoneInterval");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitTimeZoneString(
    PrestoSqlParser::TimeZoneStringContext* ctx) {
  trace("visitTimeZoneString");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitComparisonOperator(
    PrestoSqlParser::ComparisonOperatorContext* ctx) {
  trace("visitComparisonOperator");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitComparisonQuantifier(
    PrestoSqlParser::ComparisonQuantifierContext* ctx) {
  trace("visitComparisonQuantifier");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitBooleanValue(
    PrestoSqlParser::BooleanValueContext* ctx) {
  trace("visitBooleanValue");
  return visitChildren(ctx);
}

namespace {

IntervalLiteral::Sign toIntervalSign(antlr4::Token* token) {
  if (token == nullptr) {
    return IntervalLiteral::Sign::kPositive;
  }

  switch (token->getType()) {
    case PrestoSqlParser::PLUS:
      return IntervalLiteral::Sign::kPositive;
    case PrestoSqlParser::MINUS:
      return IntervalLiteral::Sign::kNegative;
    default:
      VELOX_USER_FAIL("Unsupported sign: {}", token->getText());
  }
}

IntervalLiteral::IntervalField toIntervalField(antlr4::Token* token) {
  switch (token->getType()) {
    case PrestoSqlParser::YEAR:
      return IntervalLiteral::IntervalField::kYear;
    case PrestoSqlParser::MONTH:
      return IntervalLiteral::IntervalField::kMonth;
    case PrestoSqlParser::DAY:
      return IntervalLiteral::IntervalField::kDay;
    case PrestoSqlParser::HOUR:
      return IntervalLiteral::IntervalField::kHour;
    case PrestoSqlParser::MINUTE:
      return IntervalLiteral::IntervalField::kMinute;
    case PrestoSqlParser::SECOND:
      return IntervalLiteral::IntervalField::kSecond;
    default:
      VELOX_USER_FAIL("Unsupported interval field: {}", token->getText());
  }
}
} // namespace

antlrcpp::Any AstBuilder::visitInterval(PrestoSqlParser::IntervalContext* ctx) {
  trace("visitInterval");

  std::optional<IntervalLiteral::IntervalField> to;
  if (ctx->to != nullptr) {
    to = toIntervalField(ctx->to->start);
  }

  return std::static_pointer_cast<Expression>(std::make_shared<IntervalLiteral>(
      getLocation(ctx),
      visitExpression(ctx->string())->as<StringLiteral>()->value(),
      toIntervalSign(ctx->sign),
      toIntervalField(ctx->from->start),
      to));
}

antlrcpp::Any AstBuilder::visitIntervalField(
    PrestoSqlParser::IntervalFieldContext* ctx) {
  trace("visitIntervalField");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitNormalForm(
    PrestoSqlParser::NormalFormContext* ctx) {
  trace("visitNormalForm");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitTypes(PrestoSqlParser::TypesContext* ctx) {
  trace("visitTypes");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitType(PrestoSqlParser::TypeContext* ctx) {
  trace("visitType");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitTypeParameter(
    PrestoSqlParser::TypeParameterContext* ctx) {
  trace("visitTypeParameter");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitBaseType(PrestoSqlParser::BaseTypeContext* ctx) {
  trace("visitBaseType");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitWhenClause(
    PrestoSqlParser::WhenClauseContext* ctx) {
  trace("visitWhenClause");

  return std::make_shared<WhenClause>(
      getLocation(ctx),
      visitTyped<Expression>(ctx->condition),
      visitTyped<Expression>(ctx->result));
}

antlrcpp::Any AstBuilder::visitFilter(PrestoSqlParser::FilterContext* ctx) {
  trace("visitFilter");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitOver(PrestoSqlParser::OverContext* ctx) {
  trace("visitOver");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitWindowFrame(
    PrestoSqlParser::WindowFrameContext* ctx) {
  trace("visitWindowFrame");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitUnboundedFrame(
    PrestoSqlParser::UnboundedFrameContext* ctx) {
  trace("visitUnboundedFrame");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCurrentRowBound(
    PrestoSqlParser::CurrentRowBoundContext* ctx) {
  trace("visitCurrentRowBound");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitBoundedFrame(
    PrestoSqlParser::BoundedFrameContext* ctx) {
  trace("visitBoundedFrame");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitUpdateAssignment(
    PrestoSqlParser::UpdateAssignmentContext* ctx) {
  trace("visitUpdateAssignment");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitExplainFormat(
    PrestoSqlParser::ExplainFormatContext* ctx) {
  trace("visitExplainFormat");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitExplainType(
    PrestoSqlParser::ExplainTypeContext* ctx) {
  trace("visitExplainType");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitIsolationLevel(
    PrestoSqlParser::IsolationLevelContext* ctx) {
  trace("visitIsolationLevel");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitTransactionAccessMode(
    PrestoSqlParser::TransactionAccessModeContext* ctx) {
  trace("visitTransactionAccessMode");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitReadUncommitted(
    PrestoSqlParser::ReadUncommittedContext* ctx) {
  trace("visitReadUncommitted");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitReadCommitted(
    PrestoSqlParser::ReadCommittedContext* ctx) {
  trace("visitReadCommitted");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRepeatableRead(
    PrestoSqlParser::RepeatableReadContext* ctx) {
  trace("visitRepeatableRead");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSerializable(
    PrestoSqlParser::SerializableContext* ctx) {
  trace("visitSerializable");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitPositionalArgument(
    PrestoSqlParser::PositionalArgumentContext* ctx) {
  trace("visitPositionalArgument");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitNamedArgument(
    PrestoSqlParser::NamedArgumentContext* ctx) {
  trace("visitNamedArgument");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitPrivilege(
    PrestoSqlParser::PrivilegeContext* ctx) {
  trace("visitPrivilege");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitQualifiedName(
    PrestoSqlParser::QualifiedNameContext* ctx) {
  trace("visitQualifiedName");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitTableVersion(
    PrestoSqlParser::TableVersionContext* ctx) {
  trace("visitTableVersion");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitTableversionasof(
    PrestoSqlParser::TableversionasofContext* ctx) {
  trace("visitTableversionasof");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitTableversionbefore(
    PrestoSqlParser::TableversionbeforeContext* ctx) {
  trace("visitTableversionbefore");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCurrentUserGrantor(
    PrestoSqlParser::CurrentUserGrantorContext* ctx) {
  trace("visitCurrentUserGrantor");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitCurrentRoleGrantor(
    PrestoSqlParser::CurrentRoleGrantorContext* ctx) {
  trace("visitCurrentRoleGrantor");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitSpecifiedPrincipal(
    PrestoSqlParser::SpecifiedPrincipalContext* ctx) {
  trace("visitSpecifiedPrincipal");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitUserPrincipal(
    PrestoSqlParser::UserPrincipalContext* ctx) {
  trace("visitUserPrincipal");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRolePrincipal(
    PrestoSqlParser::RolePrincipalContext* ctx) {
  trace("visitRolePrincipal");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitUnspecifiedPrincipal(
    PrestoSqlParser::UnspecifiedPrincipalContext* ctx) {
  trace("visitUnspecifiedPrincipal");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitRoles(PrestoSqlParser::RolesContext* ctx) {
  trace("visitRoles");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitQuotedIdentifier(
    PrestoSqlParser::QuotedIdentifierContext* ctx) {
  trace("visitQuotedIdentifier");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitBackQuotedIdentifier(
    PrestoSqlParser::BackQuotedIdentifierContext* ctx) {
  trace("visitBackQuotedIdentifier");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDigitIdentifier(
    PrestoSqlParser::DigitIdentifierContext* ctx) {
  trace("visitDigitIdentifier");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitDecimalLiteral(
    PrestoSqlParser::DecimalLiteralContext* ctx) {
  trace("visitDecimalLiteral");

  // TODO Introduce ParsingOptions to allow parsing decimal as either double or
  // decimal.

  return std::static_pointer_cast<Expression>(std::make_shared<DoubleLiteral>(
      getLocation(ctx), std::stod(ctx->getText())));
}

antlrcpp::Any AstBuilder::visitDoubleLiteral(
    PrestoSqlParser::DoubleLiteralContext* ctx) {
  trace("visitDoubleLiteral");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitIntegerLiteral(
    PrestoSqlParser::IntegerLiteralContext* ctx) {
  trace("visitIntegerLiteral");

  int64_t value = std::stoll(ctx->getText());

  return std::static_pointer_cast<Expression>(
      std::make_shared<LongLiteral>(getLocation(ctx), value));
}

antlrcpp::Any AstBuilder::visitConstraintSpecification(
    PrestoSqlParser::ConstraintSpecificationContext* ctx) {
  trace("visitConstraintSpecification");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitNamedConstraintSpecification(
    PrestoSqlParser::NamedConstraintSpecificationContext* ctx) {
  trace("visitNamedConstraintSpecification");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitUnnamedConstraintSpecification(
    PrestoSqlParser::UnnamedConstraintSpecificationContext* ctx) {
  trace("visitUnnamedConstraintSpecification");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitConstraintType(
    PrestoSqlParser::ConstraintTypeContext* ctx) {
  trace("visitConstraintType");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitConstraintQualifiers(
    PrestoSqlParser::ConstraintQualifiersContext* ctx) {
  trace("visitConstraintQualifiers");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitConstraintQualifier(
    PrestoSqlParser::ConstraintQualifierContext* ctx) {
  trace("visitConstraintQualifier");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitConstraintRely(
    PrestoSqlParser::ConstraintRelyContext* ctx) {
  trace("visitConstraintRely");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitConstraintEnabled(
    PrestoSqlParser::ConstraintEnabledContext* ctx) {
  trace("visitConstraintEnabled");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitConstraintEnforced(
    PrestoSqlParser::ConstraintEnforcedContext* ctx) {
  trace("visitConstraintEnforced");
  return visitChildren(ctx);
}

antlrcpp::Any AstBuilder::visitNonReserved(
    PrestoSqlParser::NonReservedContext* ctx) {
  trace("visitNonReserved");
  return visitChildren(ctx);
}

} // namespace axiom::sql::presto
