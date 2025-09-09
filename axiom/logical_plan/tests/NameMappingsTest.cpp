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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/logical_plan/NameAllocator.h"
#include "axiom/logical_plan/NameMappings.h"

namespace facebook::axiom::logical_plan {

TEST(NameMappingsTest, basic) {
  NameAllocator allocator;

  auto newName = [&](const std::string& name) {
    return allocator.newName(name);
  };

  NameMappings mappings;

  auto reverseLookup = [&](const std::string& id) {
    auto names = mappings.reverseLookup(id);

    std::vector<std::string> strings;
    strings.reserve(names.size());
    for (auto& name : names) {
      strings.push_back(name.toString());
    }

    return strings;
  };

  auto makeNamesEq = [&](std::initializer_list<std::string> names) {
    return testing::UnorderedElementsAreArray(names);
  };

  {
    mappings.add("a", newName("a"));
    mappings.add("b", newName("b"));
    mappings.add("c", newName("c"));

    EXPECT_EQ(mappings.lookup("a"), "a");
    EXPECT_EQ(mappings.lookup("b"), "b");
    EXPECT_EQ(mappings.lookup("c"), "c");

    mappings.setAlias("t");

    EXPECT_EQ(mappings.lookup("t", "a"), "a");
    EXPECT_EQ(mappings.lookup("t", "b"), "b");
    EXPECT_EQ(mappings.lookup("t", "c"), "c");

    EXPECT_THAT(reverseLookup("a"), makeNamesEq({"a", "t.a"}));
    EXPECT_THAT(reverseLookup("b"), makeNamesEq({"b", "t.b"}));
    EXPECT_THAT(reverseLookup("c"), makeNamesEq({"c", "t.c"}));

    NameMappings other;
    other.add("a", newName("a"));
    other.add("c", newName("c"));
    other.add("e", newName("e"));

    mappings.merge(other);

    // "a" and "c" are no longer accessible w/o the alias. "a" from other is not
    // accessible at all.

    EXPECT_EQ(mappings.lookup("a"), std::nullopt);
    EXPECT_EQ(mappings.lookup("t", "a"), "a");

    EXPECT_EQ(mappings.lookup("b"), "b");
    EXPECT_EQ(mappings.lookup("t", "b"), "b");

    EXPECT_EQ(mappings.lookup("c"), std::nullopt);
    EXPECT_EQ(mappings.lookup("t", "c"), "c");

    EXPECT_EQ(mappings.lookup("e"), "e");

    EXPECT_THAT(reverseLookup("a"), makeNamesEq({"t.a"}));
    EXPECT_THAT(reverseLookup("b"), makeNamesEq({"b", "t.b"}));
    EXPECT_THAT(reverseLookup("c"), makeNamesEq({"t.c"}));
    EXPECT_THAT(reverseLookup("e"), makeNamesEq({"e"}));
  }

  {
    allocator.reset();
    mappings.reset();

    mappings.add("a", newName("a"));
    mappings.add("b", newName("b"));
    mappings.add("c", newName("c"));
    mappings.setAlias("t");

    NameMappings other;
    other.add("a", newName("a"));
    other.add("c", newName("c"));
    other.add("e", newName("e"));
    other.setAlias("u");
    mappings.merge(other);

    // "a" and "c" are no longer accessible w/o the alias.

    EXPECT_EQ(mappings.lookup("a"), std::nullopt);
    EXPECT_EQ(mappings.lookup("t", "a"), "a");
    EXPECT_EQ(mappings.lookup("u", "a"), "a_0");

    EXPECT_EQ(mappings.lookup("b"), "b");
    EXPECT_EQ(mappings.lookup("t", "b"), "b");
    EXPECT_EQ(mappings.lookup("u", "b"), std::nullopt);

    EXPECT_EQ(mappings.lookup("c"), std::nullopt);
    EXPECT_EQ(mappings.lookup("t", "c"), "c");
    EXPECT_EQ(mappings.lookup("u", "c"), "c_1");

    EXPECT_EQ(mappings.lookup("e"), "e");
    EXPECT_EQ(mappings.lookup("t", "e"), std::nullopt);
    EXPECT_EQ(mappings.lookup("u", "e"), "e");

    EXPECT_THAT(reverseLookup("a"), makeNamesEq({"t.a"}));
    EXPECT_THAT(reverseLookup("b"), makeNamesEq({"b", "t.b"}));
    EXPECT_THAT(reverseLookup("c"), makeNamesEq({"t.c"}));

    EXPECT_THAT(reverseLookup("a_0"), makeNamesEq({"u.a"}));
    EXPECT_THAT(reverseLookup("c_1"), makeNamesEq({"u.c"}));
    EXPECT_THAT(reverseLookup("e"), makeNamesEq({"e", "u.e"}));

    mappings.setAlias("v");

    // Only b and e are still accessible.

    EXPECT_EQ(mappings.lookup("a"), std::nullopt);
    EXPECT_EQ(mappings.lookup("b"), "b");
    EXPECT_EQ(mappings.lookup("v", "b"), "b");
    EXPECT_EQ(mappings.lookup("c"), std::nullopt);
    EXPECT_EQ(mappings.lookup("v", "e"), "e");

    EXPECT_THAT(reverseLookup("b"), makeNamesEq({"b", "v.b"}));
    EXPECT_THAT(reverseLookup("e"), makeNamesEq({"e", "v.e"}));
  }
}

} // namespace facebook::axiom::logical_plan
