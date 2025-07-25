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

#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/common/base/SuccinctPrinter.h"

namespace facebook::velox::optimizer {

// Returns the cost and cardinality ('unitCost' and 'fanout') for 'conjuncts'.
Cost filterCost(CPSpan<Expr> conjuncts);

/// Returns 'conjuncts' with all items that are common between all disjuncts of
/// each OR are pulled to top level.
ExprVector extractCommonConjuncts(ExprVector conjuncts) {}

// Extracts an OR that can be resolved for 'table'.  This has a result
// if each disjunct of 'or' is an and that specifies some condition
// that can be resolved within 'table'.
disjunctsForTable(ExprPtr or, PlanObjectCP table);

} // namespace facebook::velox::optimizer
