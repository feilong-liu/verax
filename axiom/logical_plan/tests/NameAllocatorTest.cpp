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

#include <gtest/gtest.h>

#include "axiom/logical_plan/PlanBuilder.h"

namespace facebook::velox::logical_plan {

TEST(NameAllocatorTest, basic) {
  NameAllocator allocator;
  EXPECT_EQ(allocator.newName("foo"), "foo");
  EXPECT_EQ(allocator.newName("foo"), "foo_0");

  EXPECT_EQ(allocator.newName("bar"), "bar");
  EXPECT_EQ(allocator.newName("bar"), "bar_1");

  EXPECT_EQ(allocator.newName("foo_0"), "foo_2");

  EXPECT_EQ(allocator.newName("foo_bar"), "foo_bar");
  EXPECT_EQ(allocator.newName("foo_bar"), "foo_bar_3");
}

} // namespace facebook::velox::logical_plan
