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

    mappings.setAlias("t", {"a", "b", "c"});

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
    mappings.setAlias("t", {"a", "b", "c"});

    NameMappings other;
    other.add("a", newName("a"));
    other.add("c", newName("c"));
    other.add("e", newName("e"));
    other.setAlias("u", {"a_0", "c_1", "e"});
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

    mappings.setAlias("v", {"a", "b", "c", "a_0", "c_1", "e"});

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

TEST(NameMappingsTest, enableUnqualifiedAccess) {
  NameMappings mappings;
  mappings.add(
      NameMappings::QualifiedName{.alias = "n1", .name = "n_nationkey"},
      "nationkey1");
  mappings.add(
      NameMappings::QualifiedName{.alias = "n1", .name = "n_name"}, "name1");

  ASSERT_FALSE(mappings.lookup("n_nationkey").has_value());
  ASSERT_FALSE(mappings.lookup("n_name").has_value());

  mappings.enableUnqualifiedAccess();
  ASSERT_TRUE(mappings.lookup("n_name").has_value());
  ASSERT_EQ("name1", mappings.lookup("n_name").value());

  ASSERT_TRUE(mappings.lookup("n_nationkey").has_value());
  ASSERT_EQ("nationkey1", mappings.lookup("n_nationkey").value());
}

// Verifies that chained merges don't re-introduce unqualified access to
// ambiguous names. After merge(a, b) removes unqualified "x" (ambiguous),
// merge(result, c) must not re-add c's unqualified "x".
TEST(NameMappingsTest, chainedMerge) {
  NameMappings a;
  a.add("x", "x_a");
  a.setAlias("a", {"x_a"});

  NameMappings b;
  b.add("x", "x_b");
  b.setAlias("b", {"x_b"});

  a.merge(b);

  // After first merge: unqualified "x" removed, qualified a.x and b.x remain.
  EXPECT_EQ(a.lookup("x"), std::nullopt);
  EXPECT_EQ(a.lookup("a", "x"), "x_a");
  EXPECT_EQ(a.lookup("b", "x"), "x_b");

  NameMappings c;
  c.add("x", "x_c");
  c.setAlias("c", {"x_c"});

  a.merge(c);

  // After second merge: unqualified "x" must still be absent — "x" is
  // ambiguous across all three tables.
  EXPECT_EQ(a.lookup("x"), std::nullopt);
  EXPECT_EQ(a.lookup("a", "x"), "x_a");
  EXPECT_EQ(a.lookup("b", "x"), "x_b");
  EXPECT_EQ(a.lookup("c", "x"), "x_c");
}

namespace {
NameMappings makeTable(
    const std::optional<std::string>& alias,
    const std::string& id) {
  NameMappings mappings;
  mappings.add("x", id);
  if (alias.has_value()) {
    mappings.setAlias(alias.value(), {id});
  }
  return mappings;
}
} // namespace

// Verifies that merging a relation that is itself a merge keeps each qualified
// name bound to its own column, and makes the shared unqualified name
// ambiguous.
TEST(NameMappingsTest, nestedMerge) {
  // t JOIN (u JOIN v), with and without an alias on the left side.
  for (const auto& leftAlias :
       {std::optional<std::string>{"t"}, std::optional<std::string>{}}) {
    SCOPED_TRACE(leftAlias.value_or("<no alias>"));
    auto left = makeTable(leftAlias, "x_t");
    auto right = makeTable("u", "x_u");
    right.merge(makeTable("v", "x_v"));

    left.merge(right);

    EXPECT_EQ(left.lookup("x"), std::nullopt);
    EXPECT_EQ(left.lookup("u", "x"), "x_u");
    EXPECT_EQ(left.lookup("v", "x"), "x_v");
    if (leftAlias.has_value()) {
      EXPECT_EQ(left.lookup(leftAlias.value(), "x"), "x_t");
    }
  }

  // No relation has an alias, so the right side has no name for 'x'.
  {
    auto left = makeTable(std::nullopt, "x_t");
    auto right = makeTable(std::nullopt, "x_u");
    right.merge(makeTable(std::nullopt, "x_v"));

    left.merge(right);

    EXPECT_EQ(left.lookup("x"), std::nullopt);
  }

  // The right side reaches 'x' only through aliases and records no ambiguity,
  // as after a USING join rebuilds its names.
  {
    auto left = makeTable("t", "x_t");
    NameMappings right;
    right.add(NameMappings::QualifiedName{.alias = "u", .name = "x"}, "x_u");
    right.add(NameMappings::QualifiedName{.alias = "v", .name = "x"}, "x_v");

    left.merge(right);

    EXPECT_EQ(left.lookup("x"), std::nullopt);
    EXPECT_EQ(left.lookup("t", "x"), "x_t");
    EXPECT_EQ(left.lookup("u", "x"), "x_u");
    EXPECT_EQ(left.lookup("v", "x"), "x_v");
  }

  // (t JOIN (u JOIN v)) w JOIN s: the ambiguity survives the alias and the
  // next merge.
  {
    auto right = makeTable("u", "x_u");
    right.merge(makeTable("v", "x_v"));

    NameMappings left;
    left.add("y", "y_t");
    left.setAlias("t", {"y_t"});
    left.merge(right);
    left.setAlias("w", {"y_t", "x_u", "x_v"});

    left.merge(makeTable("s", "x_s"));

    EXPECT_EQ(left.lookup("x"), std::nullopt);
    EXPECT_EQ(left.lookup("s", "x"), "x_s");
    EXPECT_EQ(left.lookup("y"), "y_t");
  }

  // A name only one side uses stays reachable without an alias.
  {
    NameMappings left;
    left.add("y", "y_t");
    left.setAlias("t", {"y_t"});

    left.merge(makeTable("u", "x_u"));

    EXPECT_EQ(left.lookup("x"), "x_u");
    EXPECT_EQ(left.lookup("y"), "y_t");
  }
}

// Verifies that an alias hides the aliases inside it, ambiguity included. In
// t JOIN ((t JOIN t) u), 't.x' names the outer relation's column, and 'u.x' is
// ambiguous.
TEST(NameMappingsTest, hiddenAlias) {
  const auto makeInner = [] {
    auto inner = makeTable("t", "x_t1");
    inner.merge(makeTable("t", "x_t2"));
    return inner;
  };

  {
    auto right = makeInner();
    right.setAlias("u", {"x_t1", "x_t2"});

    auto left = makeTable("t", "x_t");
    left.merge(right);

    EXPECT_EQ(left.lookup("t", "x"), "x_t");
    EXPECT_EQ(left.lookup("u", "x"), std::nullopt);
    EXPECT_TRUE(left.isAmbiguous({.alias = "u", .name = "x"}));
    EXPECT_EQ(left.lookup("x"), std::nullopt);
  }

  {
    auto left = makeInner();
    left.setAlias("u", {"x_t1", "x_t2"});

    left.merge(makeTable("t", "x_t"));

    EXPECT_EQ(left.lookup("t", "x"), "x_t");
    EXPECT_TRUE(left.isAmbiguous({.alias = "u", .name = "x"}));
  }

  // A derived table hides every alias inside it.
  {
    auto left = makeInner();
    left.clearAliases();

    left.merge(makeTable("t", "x_t"));

    EXPECT_EQ(left.lookup("t", "x"), "x_t");
    EXPECT_TRUE(left.isAmbiguous({.alias = std::nullopt, .name = "x"}));
  }
}

TEST(NameMappingsTest, markAmbiguous) {
  NameMappings mappings;
  mappings.add("x", "x_1");
  mappings.markAmbiguous({.alias = std::nullopt, .name = "x"});

  EXPECT_EQ(mappings.lookup("x"), std::nullopt);
  EXPECT_TRUE(mappings.isAmbiguous({.alias = std::nullopt, .name = "x"}));

  // A name that resolved to more than one column resolves to none, whatever
  // order the columns claiming it are added in.
  mappings.add("x", "x_2");
  EXPECT_EQ(mappings.lookup("x"), std::nullopt);

  // Merging in another relation does not reinstate it either.
  NameMappings other;
  other.add("x", "x_3");
  mappings.merge(other);
  EXPECT_EQ(mappings.lookup("x"), std::nullopt);

  // An unrelated name is unaffected, and 'reset' clears the mark.
  mappings.add("y", "y_1");
  EXPECT_EQ(mappings.lookup("y"), "y_1");

  mappings.reset();
  mappings.add("x", "x_4");
  EXPECT_EQ(mappings.lookup("x"), "x_4");
  EXPECT_FALSE(mappings.isAmbiguous({.alias = std::nullopt, .name = "x"}));
}

// Verifies that a name ambiguous on either side of a merge stays ambiguous, so
// a later merge cannot make it resolve. In t JOIN (u JOIN u) JOIN u, 'u.x'
// names three columns.
TEST(NameMappingsTest, mergedMarks) {
  auto right = makeTable("u", "x_u1");
  right.merge(makeTable("u", "x_u2"));

  auto mappings = makeTable("t", "x_t");
  mappings.merge(right);
  mappings.merge(makeTable("u", "x_u3"));

  EXPECT_EQ(mappings.lookup("u", "x"), std::nullopt);
  EXPECT_TRUE(mappings.isAmbiguous({.alias = "u", .name = "x"}));
  EXPECT_EQ(mappings.lookup("t", "x"), "x_t");
}

// Verifies that a scope rebuilt with all of another scope's columns keeps that
// scope's ambiguous names ambiguous, so a column added later cannot claim one.
TEST(NameMappingsTest, copyAmbiguousNames) {
  auto source = makeTable("t", "x_t");
  source.merge(makeTable("u", "x_u"));

  NameMappings rebuilt;
  rebuilt.add(NameMappings::QualifiedName{.alias = "t", .name = "x"}, "x_t");
  rebuilt.add(NameMappings::QualifiedName{.alias = "u", .name = "x"}, "x_u");
  rebuilt.copyAmbiguousNames(source);
  rebuilt.add("x", "x_new");

  EXPECT_EQ(rebuilt.lookup("x"), std::nullopt);
  EXPECT_TRUE(rebuilt.isAmbiguous({.alias = std::nullopt, .name = "x"}));
  EXPECT_EQ(rebuilt.lookup("t", "x"), "x_t");
  EXPECT_EQ(rebuilt.lookup("u", "x"), "x_u");
}

// Verifies that an alias names every column of the relation it is set on,
// including columns that no name reaches because their names are ambiguous.
TEST(NameMappingsTest, idsWithAlias) {
  // (t JOIN u) v: 'x' names both columns, which only 't.x' and 'u.x' reach
  // until the alias hides those.
  auto mappings = makeTable("t", "x_t");
  mappings.merge(makeTable("u", "x_u"));
  mappings.setAlias("v", {"x_t", "x_u"});

  EXPECT_THAT(
      mappings.idsWithAlias("v"), testing::UnorderedElementsAre("x_t", "x_u"));
  EXPECT_THAT(mappings.idsWithAlias("t"), testing::IsEmpty());

  // A later merge keeps the alias's columns and adds the other side's.
  mappings.merge(makeTable("w", "x_w"));
  EXPECT_THAT(
      mappings.idsWithAlias("v"), testing::UnorderedElementsAre("x_t", "x_u"));
  EXPECT_THAT(mappings.idsWithAlias("w"), testing::UnorderedElementsAre("x_w"));

  // A derived table hides every alias.
  mappings.clearAliases();
  EXPECT_THAT(mappings.idsWithAlias("v"), testing::IsEmpty());

  // Without aliases, 'x' names both columns, so neither column has a name.
  NameMappings unnamed;
  unnamed.add("x", "x_1");
  NameMappings other;
  other.add("x", "x_2");
  unnamed.merge(other);
  unnamed.setAlias("v", {"x_1", "x_2"});
  EXPECT_THAT(
      unnamed.idsWithAlias("v"), testing::UnorderedElementsAre("x_1", "x_2"));
}

} // namespace facebook::axiom::logical_plan
