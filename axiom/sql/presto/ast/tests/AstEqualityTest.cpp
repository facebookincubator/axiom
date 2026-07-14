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
#include "axiom/sql/presto/ast/AstExpressions.h"
#include "axiom/sql/presto/ast/AstFunctions.h"
#include "axiom/sql/presto/ast/AstLiterals.h"
#include "axiom/sql/presto/ast/AstRelations.h"
#include "axiom/sql/presto/ast/AstSupport.h"

namespace axiom::sql::presto {
namespace {

TEST(AstEqualityTest, longLiteralEqualsSelf) {
  auto a = std::make_shared<LongLiteral>(NodeLocation{0, 0}, 42);
  EXPECT_TRUE(*a == *a);
  EXPECT_EQ(a->hash(), a->hash());
}

TEST(AstEqualityTest, longLiteralIgnoresNodeLocation) {
  auto a = std::make_shared<LongLiteral>(NodeLocation{1, 1}, 42);
  auto b = std::make_shared<LongLiteral>(NodeLocation{99, 99}, 42);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, deepEqualHandlesNullPtrs) {
  NodePtr a, b;
  EXPECT_TRUE(Node::deepEqual(a, b));
}

TEST(AstEqualityTest, deepEqualAllHandlesVector) {
  std::vector<NodePtr> a, b;
  a.push_back(std::make_shared<LongLiteral>(NodeLocation{0, 0}, 1));
  b.push_back(std::make_shared<LongLiteral>(NodeLocation{0, 0}, 1));
  EXPECT_TRUE(Node::deepEqualAll(a, b));
}

TEST(AstEqualityTest, deepEqualAllRejectsLengthMismatch) {
  std::vector<NodePtr> a, b;
  a.push_back(std::make_shared<LongLiteral>(NodeLocation{0, 0}, 1));
  EXPECT_FALSE(Node::deepEqualAll(a, b));
}

TEST(AstEqualityTest, booleanLiteralEquals) {
  auto a = std::make_shared<BooleanLiteral>(NodeLocation{0, 0}, true);
  auto b = std::make_shared<BooleanLiteral>(NodeLocation{5, 5}, true);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, booleanLiteralNotEqual) {
  auto a = std::make_shared<BooleanLiteral>(NodeLocation{0, 0}, true);
  auto b = std::make_shared<BooleanLiteral>(NodeLocation{0, 0}, false);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, stringLiteralEquals) {
  auto a = std::make_shared<StringLiteral>(NodeLocation{0, 0}, "hello");
  auto b = std::make_shared<StringLiteral>(NodeLocation{5, 5}, "hello");
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, stringLiteralNotEqual) {
  auto a = std::make_shared<StringLiteral>(NodeLocation{0, 0}, "hello");
  auto b = std::make_shared<StringLiteral>(NodeLocation{0, 0}, "world");
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, binaryLiteralEquals) {
  auto a = std::make_shared<BinaryLiteral>(NodeLocation{0, 0}, "deadbeef");
  auto b = std::make_shared<BinaryLiteral>(NodeLocation{5, 5}, "deadbeef");
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, binaryLiteralNotEqual) {
  auto a = std::make_shared<BinaryLiteral>(NodeLocation{0, 0}, "deadbeef");
  auto b = std::make_shared<BinaryLiteral>(NodeLocation{0, 0}, "cafebabe");
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, charLiteralEquals) {
  auto a = std::make_shared<CharLiteral>(NodeLocation{0, 0}, "abc");
  auto b = std::make_shared<CharLiteral>(NodeLocation{5, 5}, "abc");
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, charLiteralNotEqual) {
  auto a = std::make_shared<CharLiteral>(NodeLocation{0, 0}, "abc");
  auto b = std::make_shared<CharLiteral>(NodeLocation{0, 0}, "xyz");
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, doubleLiteralEquals) {
  auto a = std::make_shared<DoubleLiteral>(NodeLocation{0, 0}, 3.14);
  auto b = std::make_shared<DoubleLiteral>(NodeLocation{5, 5}, 3.14);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, doubleLiteralNotEqual) {
  auto a = std::make_shared<DoubleLiteral>(NodeLocation{0, 0}, 3.14);
  auto b = std::make_shared<DoubleLiteral>(NodeLocation{0, 0}, 2.71);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, decimalLiteralEquals) {
  auto a = std::make_shared<DecimalLiteral>(NodeLocation{0, 0}, "1.23");
  auto b = std::make_shared<DecimalLiteral>(NodeLocation{5, 5}, "1.23");
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, decimalLiteralNotEqual) {
  auto a = std::make_shared<DecimalLiteral>(NodeLocation{0, 0}, "1.23");
  auto b = std::make_shared<DecimalLiteral>(NodeLocation{0, 0}, "4.56");
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, genericLiteralEquals) {
  auto type1 = std::make_shared<TypeSignature>(NodeLocation{0, 0}, "varchar");
  auto type2 = std::make_shared<TypeSignature>(NodeLocation{5, 5}, "varchar");
  auto a = std::make_shared<GenericLiteral>(NodeLocation{0, 0}, type1, "value");
  auto b = std::make_shared<GenericLiteral>(NodeLocation{5, 5}, type2, "value");
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, genericLiteralNotEqualValue) {
  auto type = std::make_shared<TypeSignature>(NodeLocation{0, 0}, "varchar");
  auto a = std::make_shared<GenericLiteral>(NodeLocation{0, 0}, type, "value");
  auto b = std::make_shared<GenericLiteral>(NodeLocation{0, 0}, type, "other");
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, nullLiteralAlwaysEqual) {
  auto a = std::make_shared<NullLiteral>(NodeLocation{0, 0});
  auto b = std::make_shared<NullLiteral>(NodeLocation{5, 5});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, timeLiteralEquals) {
  auto a = std::make_shared<TimeLiteral>(NodeLocation{0, 0}, "12:00:00");
  auto b = std::make_shared<TimeLiteral>(NodeLocation{5, 5}, "12:00:00");
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, timeLiteralNotEqual) {
  auto a = std::make_shared<TimeLiteral>(NodeLocation{0, 0}, "12:00:00");
  auto b = std::make_shared<TimeLiteral>(NodeLocation{0, 0}, "13:00:00");
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, timestampLiteralEquals) {
  auto a = std::make_shared<TimestampLiteral>(
      NodeLocation{0, 0}, "2024-01-01 00:00:00");
  auto b = std::make_shared<TimestampLiteral>(
      NodeLocation{5, 5}, "2024-01-01 00:00:00");
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, timestampLiteralNotEqual) {
  auto a = std::make_shared<TimestampLiteral>(
      NodeLocation{0, 0}, "2024-01-01 00:00:00");
  auto b = std::make_shared<TimestampLiteral>(
      NodeLocation{0, 0}, "2024-02-02 00:00:00");
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, intervalLiteralEquals) {
  auto a = std::make_shared<IntervalLiteral>(
      NodeLocation{0, 0},
      "5",
      IntervalLiteral::Sign::kPositive,
      IntervalLiteral::IntervalField::kDay);
  auto b = std::make_shared<IntervalLiteral>(
      NodeLocation{5, 5},
      "5",
      IntervalLiteral::Sign::kPositive,
      IntervalLiteral::IntervalField::kDay);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, intervalLiteralEqualsWithEndField) {
  auto a = std::make_shared<IntervalLiteral>(
      NodeLocation{0, 0},
      "1",
      IntervalLiteral::Sign::kPositive,
      IntervalLiteral::IntervalField::kYear,
      IntervalLiteral::IntervalField::kMonth);
  auto b = std::make_shared<IntervalLiteral>(
      NodeLocation{5, 5},
      "1",
      IntervalLiteral::Sign::kPositive,
      IntervalLiteral::IntervalField::kYear,
      IntervalLiteral::IntervalField::kMonth);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, intervalLiteralNotEqualValue) {
  auto a = std::make_shared<IntervalLiteral>(
      NodeLocation{0, 0},
      "5",
      IntervalLiteral::Sign::kPositive,
      IntervalLiteral::IntervalField::kDay);
  auto b = std::make_shared<IntervalLiteral>(
      NodeLocation{0, 0},
      "7",
      IntervalLiteral::Sign::kPositive,
      IntervalLiteral::IntervalField::kDay);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, intervalLiteralNotEqualSign) {
  auto a = std::make_shared<IntervalLiteral>(
      NodeLocation{0, 0},
      "5",
      IntervalLiteral::Sign::kPositive,
      IntervalLiteral::IntervalField::kDay);
  auto b = std::make_shared<IntervalLiteral>(
      NodeLocation{0, 0},
      "5",
      IntervalLiteral::Sign::kNegative,
      IntervalLiteral::IntervalField::kDay);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, intervalLiteralNotEqualStartField) {
  auto a = std::make_shared<IntervalLiteral>(
      NodeLocation{0, 0},
      "5",
      IntervalLiteral::Sign::kPositive,
      IntervalLiteral::IntervalField::kDay);
  auto b = std::make_shared<IntervalLiteral>(
      NodeLocation{0, 0},
      "5",
      IntervalLiteral::Sign::kPositive,
      IntervalLiteral::IntervalField::kHour);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, intervalLiteralNotEqualEndField) {
  auto a = std::make_shared<IntervalLiteral>(
      NodeLocation{0, 0},
      "1",
      IntervalLiteral::Sign::kPositive,
      IntervalLiteral::IntervalField::kYear,
      IntervalLiteral::IntervalField::kMonth);
  auto b = std::make_shared<IntervalLiteral>(
      NodeLocation{0, 0},
      "1",
      IntervalLiteral::Sign::kPositive,
      IntervalLiteral::IntervalField::kYear);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, enumLiteralEquals) {
  auto a = std::make_shared<EnumLiteral>(NodeLocation{0, 0}, "RED");
  auto b = std::make_shared<EnumLiteral>(NodeLocation{5, 5}, "RED");
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, enumLiteralNotEqual) {
  auto a = std::make_shared<EnumLiteral>(NodeLocation{0, 0}, "RED");
  auto b = std::make_shared<EnumLiteral>(NodeLocation{0, 0}, "BLUE");
  EXPECT_FALSE(*a == *b);
}

namespace {
NodeLocation loc(int line = 0, int col = 0) {
  return NodeLocation{line, col};
}

std::shared_ptr<LongLiteral> longLit(int64_t v) {
  return std::make_shared<LongLiteral>(loc(), v);
}
} // namespace

TEST(AstEqualityTest, identifierEquals) {
  auto a = std::make_shared<Identifier>(loc(0, 0), "foo", false);
  auto b = std::make_shared<Identifier>(loc(5, 5), "foo", false);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, identifierNotEqualValue) {
  auto a = std::make_shared<Identifier>(loc(), "foo", false);
  auto b = std::make_shared<Identifier>(loc(), "bar", false);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, identifierNotEqualDelimited) {
  auto a = std::make_shared<Identifier>(loc(), "foo", false);
  auto b = std::make_shared<Identifier>(loc(), "foo", true);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, qualifiedNameEquals) {
  auto a = std::make_shared<QualifiedName>(
      loc(0, 0), std::vector<std::string>{"a", "b"});
  auto b = std::make_shared<QualifiedName>(
      loc(5, 5), std::vector<std::string>{"a", "b"});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, qualifiedNameNotEqual) {
  auto a = std::make_shared<QualifiedName>(
      loc(), std::vector<std::string>{"a", "b"});
  auto b = std::make_shared<QualifiedName>(
      loc(), std::vector<std::string>{"a", "c"});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, dereferenceExpressionEquals) {
  auto base1 = std::make_shared<Identifier>(loc(), "t", false);
  auto field1 = std::make_shared<Identifier>(loc(), "f", false);
  auto base2 = std::make_shared<Identifier>(loc(5, 5), "t", false);
  auto field2 = std::make_shared<Identifier>(loc(5, 5), "f", false);
  auto a = std::make_shared<DereferenceExpression>(loc(), base1, field1);
  auto b = std::make_shared<DereferenceExpression>(loc(5, 5), base2, field2);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, dereferenceExpressionNotEqual) {
  auto base = std::make_shared<Identifier>(loc(), "t", false);
  auto field1 = std::make_shared<Identifier>(loc(), "f", false);
  auto field2 = std::make_shared<Identifier>(loc(), "g", false);
  auto a = std::make_shared<DereferenceExpression>(loc(), base, field1);
  auto b = std::make_shared<DereferenceExpression>(loc(), base, field2);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, fieldReferenceEquals) {
  auto a = std::make_shared<FieldReference>(loc(0, 0), 3);
  auto b = std::make_shared<FieldReference>(loc(5, 5), 3);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, fieldReferenceNotEqual) {
  auto a = std::make_shared<FieldReference>(loc(), 3);
  auto b = std::make_shared<FieldReference>(loc(), 4);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, symbolReferenceEquals) {
  auto a = std::make_shared<SymbolReference>(loc(0, 0), "x");
  auto b = std::make_shared<SymbolReference>(loc(5, 5), "x");
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, symbolReferenceNotEqual) {
  auto a = std::make_shared<SymbolReference>(loc(), "x");
  auto b = std::make_shared<SymbolReference>(loc(), "y");
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, parameterEquals) {
  auto a = std::make_shared<Parameter>(loc(0, 0), 2);
  auto b = std::make_shared<Parameter>(loc(5, 5), 2);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, parameterNotEqual) {
  auto a = std::make_shared<Parameter>(loc(), 2);
  auto b = std::make_shared<Parameter>(loc(), 3);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, arithmeticBinaryExpressionEquals) {
  auto a = std::make_shared<ArithmeticBinaryExpression>(
      loc(0, 0),
      ArithmeticBinaryExpression::Operator::kAdd,
      longLit(1),
      longLit(2));
  auto b = std::make_shared<ArithmeticBinaryExpression>(
      loc(5, 5),
      ArithmeticBinaryExpression::Operator::kAdd,
      longLit(1),
      longLit(2));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, arithmeticBinaryExpressionNotEqualOperand) {
  auto a = std::make_shared<ArithmeticBinaryExpression>(
      loc(),
      ArithmeticBinaryExpression::Operator::kAdd,
      longLit(1),
      longLit(2));
  auto b = std::make_shared<ArithmeticBinaryExpression>(
      loc(),
      ArithmeticBinaryExpression::Operator::kAdd,
      longLit(1),
      longLit(3));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, arithmeticBinaryExpressionNotEqualOperator) {
  // 1 + 2 vs 1 - 2.
  auto a = std::make_shared<ArithmeticBinaryExpression>(
      loc(),
      ArithmeticBinaryExpression::Operator::kAdd,
      longLit(1),
      longLit(2));
  auto b = std::make_shared<ArithmeticBinaryExpression>(
      loc(),
      ArithmeticBinaryExpression::Operator::kSubtract,
      longLit(1),
      longLit(2));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, arithmeticUnaryExpressionEquals) {
  auto a = std::make_shared<ArithmeticUnaryExpression>(
      loc(0, 0), ArithmeticUnaryExpression::Sign::kMinus, longLit(1));
  auto b = std::make_shared<ArithmeticUnaryExpression>(
      loc(5, 5), ArithmeticUnaryExpression::Sign::kMinus, longLit(1));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, arithmeticUnaryExpressionNotEqualSign) {
  auto a = std::make_shared<ArithmeticUnaryExpression>(
      loc(), ArithmeticUnaryExpression::Sign::kMinus, longLit(1));
  auto b = std::make_shared<ArithmeticUnaryExpression>(
      loc(), ArithmeticUnaryExpression::Sign::kPlus, longLit(1));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, comparisonExpressionEquals) {
  auto a = std::make_shared<ComparisonExpression>(
      loc(0, 0),
      ComparisonExpression::Operator::kEqual,
      longLit(1),
      longLit(2));
  auto b = std::make_shared<ComparisonExpression>(
      loc(5, 5),
      ComparisonExpression::Operator::kEqual,
      longLit(1),
      longLit(2));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, comparisonExpressionNotEqualOperator) {
  // a = b vs a < b.
  auto a = std::make_shared<ComparisonExpression>(
      loc(), ComparisonExpression::Operator::kEqual, longLit(1), longLit(2));
  auto b = std::make_shared<ComparisonExpression>(
      loc(), ComparisonExpression::Operator::kLessThan, longLit(1), longLit(2));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, betweenPredicateEquals) {
  auto a = std::make_shared<BetweenPredicate>(
      loc(0, 0), longLit(5), longLit(1), longLit(10));
  auto b = std::make_shared<BetweenPredicate>(
      loc(5, 5), longLit(5), longLit(1), longLit(10));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, betweenPredicateNotEqual) {
  auto a = std::make_shared<BetweenPredicate>(
      loc(), longLit(5), longLit(1), longLit(10));
  auto b = std::make_shared<BetweenPredicate>(
      loc(), longLit(5), longLit(2), longLit(10));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, inPredicateEquals) {
  auto list1 = std::make_shared<InListExpression>(
      loc(), std::vector<ExpressionPtr>{longLit(1), longLit(2)});
  auto list2 = std::make_shared<InListExpression>(
      loc(), std::vector<ExpressionPtr>{longLit(1), longLit(2)});
  auto a = std::make_shared<InPredicate>(loc(0, 0), longLit(5), list1);
  auto b = std::make_shared<InPredicate>(loc(5, 5), longLit(5), list2);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, inPredicateNotEqual) {
  auto list = std::make_shared<InListExpression>(
      loc(), std::vector<ExpressionPtr>{longLit(1)});
  auto a = std::make_shared<InPredicate>(loc(), longLit(5), list);
  auto b = std::make_shared<InPredicate>(loc(), longLit(6), list);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, inListExpressionEquals) {
  auto a = std::make_shared<InListExpression>(
      loc(0, 0), std::vector<ExpressionPtr>{longLit(1), longLit(2)});
  auto b = std::make_shared<InListExpression>(
      loc(5, 5), std::vector<ExpressionPtr>{longLit(1), longLit(2)});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, inListExpressionNotEqual) {
  auto a = std::make_shared<InListExpression>(
      loc(), std::vector<ExpressionPtr>{longLit(1), longLit(2)});
  auto b = std::make_shared<InListExpression>(
      loc(), std::vector<ExpressionPtr>{longLit(1), longLit(3)});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, isNullPredicateEquals) {
  auto a = std::make_shared<IsNullPredicate>(loc(0, 0), longLit(1));
  auto b = std::make_shared<IsNullPredicate>(loc(5, 5), longLit(1));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, isNullPredicateNotEqual) {
  auto a = std::make_shared<IsNullPredicate>(loc(), longLit(1));
  auto b = std::make_shared<IsNullPredicate>(loc(), longLit(2));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, isNotNullPredicateEquals) {
  auto a = std::make_shared<IsNotNullPredicate>(loc(0, 0), longLit(1));
  auto b = std::make_shared<IsNotNullPredicate>(loc(5, 5), longLit(1));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, isNotNullPredicateNotEqual) {
  auto a = std::make_shared<IsNotNullPredicate>(loc(), longLit(1));
  auto b = std::make_shared<IsNotNullPredicate>(loc(), longLit(2));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, likePredicateEquals) {
  auto pat = std::make_shared<StringLiteral>(loc(), "a%");
  auto a = std::make_shared<LikePredicate>(loc(0, 0), longLit(1), pat);
  auto b = std::make_shared<LikePredicate>(loc(5, 5), longLit(1), pat);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, likePredicateNotEqualPattern) {
  auto pat1 = std::make_shared<StringLiteral>(loc(), "a%");
  auto pat2 = std::make_shared<StringLiteral>(loc(), "b%");
  auto a = std::make_shared<LikePredicate>(loc(), longLit(1), pat1);
  auto b = std::make_shared<LikePredicate>(loc(), longLit(1), pat2);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, likePredicateNotEqualEscape) {
  auto pat = std::make_shared<StringLiteral>(loc(), "a%");
  auto esc = std::make_shared<StringLiteral>(loc(), "\\");
  auto a = std::make_shared<LikePredicate>(loc(), longLit(1), pat, nullptr);
  auto b = std::make_shared<LikePredicate>(loc(), longLit(1), pat, esc);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, existsPredicateEquals) {
  auto a = std::make_shared<ExistsPredicate>(loc(0, 0), longLit(1));
  auto b = std::make_shared<ExistsPredicate>(loc(5, 5), longLit(1));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, existsPredicateNotEqual) {
  auto a = std::make_shared<ExistsPredicate>(loc(), longLit(1));
  auto b = std::make_shared<ExistsPredicate>(loc(), longLit(2));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, quantifiedComparisonExpressionEquals) {
  auto a = std::make_shared<QuantifiedComparisonExpression>(
      loc(0, 0),
      ComparisonExpression::Operator::kEqual,
      QuantifiedComparisonExpression::Quantifier::kAll,
      longLit(1),
      longLit(2));
  auto b = std::make_shared<QuantifiedComparisonExpression>(
      loc(5, 5),
      ComparisonExpression::Operator::kEqual,
      QuantifiedComparisonExpression::Quantifier::kAll,
      longLit(1),
      longLit(2));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, quantifiedComparisonExpressionNotEqualQuantifier) {
  auto a = std::make_shared<QuantifiedComparisonExpression>(
      loc(),
      ComparisonExpression::Operator::kEqual,
      QuantifiedComparisonExpression::Quantifier::kAll,
      longLit(1),
      longLit(2));
  auto b = std::make_shared<QuantifiedComparisonExpression>(
      loc(),
      ComparisonExpression::Operator::kEqual,
      QuantifiedComparisonExpression::Quantifier::kAny,
      longLit(1),
      longLit(2));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, logicalBinaryExpressionEquals) {
  auto a = std::make_shared<LogicalBinaryExpression>(
      loc(0, 0),
      LogicalBinaryExpression::Operator::kAnd,
      longLit(1),
      longLit(2));
  auto b = std::make_shared<LogicalBinaryExpression>(
      loc(5, 5),
      LogicalBinaryExpression::Operator::kAnd,
      longLit(1),
      longLit(2));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, logicalBinaryExpressionNotEqualOperator) {
  // a AND b vs a OR b.
  auto a = std::make_shared<LogicalBinaryExpression>(
      loc(), LogicalBinaryExpression::Operator::kAnd, longLit(1), longLit(2));
  auto b = std::make_shared<LogicalBinaryExpression>(
      loc(), LogicalBinaryExpression::Operator::kOr, longLit(1), longLit(2));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, notExpressionEquals) {
  auto a = std::make_shared<NotExpression>(loc(0, 0), longLit(1));
  auto b = std::make_shared<NotExpression>(loc(5, 5), longLit(1));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, notExpressionNotEqual) {
  auto a = std::make_shared<NotExpression>(loc(), longLit(1));
  auto b = std::make_shared<NotExpression>(loc(), longLit(2));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, ifExpressionEquals) {
  auto a = std::make_shared<IfExpression>(
      loc(0, 0), longLit(1), longLit(2), longLit(3));
  auto b = std::make_shared<IfExpression>(
      loc(5, 5), longLit(1), longLit(2), longLit(3));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, ifExpressionNotEqual) {
  auto a = std::make_shared<IfExpression>(loc(), longLit(1), longLit(2));
  auto b = std::make_shared<IfExpression>(loc(), longLit(1), longLit(99));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, coalesceExpressionEquals) {
  auto a = std::make_shared<CoalesceExpression>(
      loc(0, 0), std::vector<ExpressionPtr>{longLit(1), longLit(2)});
  auto b = std::make_shared<CoalesceExpression>(
      loc(5, 5), std::vector<ExpressionPtr>{longLit(1), longLit(2)});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, coalesceExpressionNotEqual) {
  auto a = std::make_shared<CoalesceExpression>(
      loc(), std::vector<ExpressionPtr>{longLit(1), longLit(2)});
  auto b = std::make_shared<CoalesceExpression>(
      loc(), std::vector<ExpressionPtr>{longLit(1), longLit(3)});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, nullIfExpressionEquals) {
  auto a =
      std::make_shared<NullIfExpression>(loc(0, 0), longLit(1), longLit(2));
  auto b =
      std::make_shared<NullIfExpression>(loc(5, 5), longLit(1), longLit(2));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, nullIfExpressionNotEqual) {
  auto a = std::make_shared<NullIfExpression>(loc(), longLit(1), longLit(2));
  auto b = std::make_shared<NullIfExpression>(loc(), longLit(1), longLit(3));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, whenClauseEquals) {
  auto a = std::make_shared<WhenClause>(loc(0, 0), longLit(1), longLit(2));
  auto b = std::make_shared<WhenClause>(loc(5, 5), longLit(1), longLit(2));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, whenClauseNotEqual) {
  auto a = std::make_shared<WhenClause>(loc(), longLit(1), longLit(2));
  auto b = std::make_shared<WhenClause>(loc(), longLit(1), longLit(3));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, searchedCaseExpressionEquals) {
  auto when1 = std::make_shared<WhenClause>(loc(), longLit(1), longLit(10));
  auto when2 = std::make_shared<WhenClause>(loc(), longLit(1), longLit(10));
  auto a = std::make_shared<SearchedCaseExpression>(
      loc(0, 0), std::vector<std::shared_ptr<WhenClause>>{when1}, longLit(99));
  auto b = std::make_shared<SearchedCaseExpression>(
      loc(5, 5), std::vector<std::shared_ptr<WhenClause>>{when2}, longLit(99));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, searchedCaseExpressionNotEqual) {
  auto when = std::make_shared<WhenClause>(loc(), longLit(1), longLit(10));
  auto a = std::make_shared<SearchedCaseExpression>(
      loc(), std::vector<std::shared_ptr<WhenClause>>{when}, longLit(99));
  auto b = std::make_shared<SearchedCaseExpression>(
      loc(), std::vector<std::shared_ptr<WhenClause>>{when}, longLit(100));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, simpleCaseExpressionEquals) {
  auto when1 = std::make_shared<WhenClause>(loc(), longLit(1), longLit(10));
  auto when2 = std::make_shared<WhenClause>(loc(), longLit(1), longLit(10));
  auto a = std::make_shared<SimpleCaseExpression>(
      loc(0, 0),
      longLit(5),
      std::vector<std::shared_ptr<WhenClause>>{when1},
      longLit(99));
  auto b = std::make_shared<SimpleCaseExpression>(
      loc(5, 5),
      longLit(5),
      std::vector<std::shared_ptr<WhenClause>>{when2},
      longLit(99));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, simpleCaseExpressionNotEqual) {
  auto when = std::make_shared<WhenClause>(loc(), longLit(1), longLit(10));
  auto a = std::make_shared<SimpleCaseExpression>(
      loc(),
      longLit(5),
      std::vector<std::shared_ptr<WhenClause>>{when},
      longLit(99));
  auto b = std::make_shared<SimpleCaseExpression>(
      loc(),
      longLit(6),
      std::vector<std::shared_ptr<WhenClause>>{when},
      longLit(99));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, tryExpressionEquals) {
  auto a = std::make_shared<TryExpression>(loc(0, 0), longLit(1));
  auto b = std::make_shared<TryExpression>(loc(5, 5), longLit(1));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, tryExpressionNotEqual) {
  auto a = std::make_shared<TryExpression>(loc(), longLit(1));
  auto b = std::make_shared<TryExpression>(loc(), longLit(2));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, functionCallEquals) {
  auto name1 =
      std::make_shared<QualifiedName>(loc(), std::vector<std::string>{"f"});
  auto name2 =
      std::make_shared<QualifiedName>(loc(), std::vector<std::string>{"f"});
  auto a = std::make_shared<FunctionCall>(
      loc(0, 0), name1, std::vector<ExpressionPtr>{longLit(1)});
  auto b = std::make_shared<FunctionCall>(
      loc(5, 5), name2, std::vector<ExpressionPtr>{longLit(1)});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, functionCallNotEqualName) {
  auto fName =
      std::make_shared<QualifiedName>(loc(), std::vector<std::string>{"f"});
  auto gName =
      std::make_shared<QualifiedName>(loc(), std::vector<std::string>{"g"});
  auto a = std::make_shared<FunctionCall>(
      loc(), fName, std::vector<ExpressionPtr>{longLit(1)});
  auto b = std::make_shared<FunctionCall>(
      loc(), gName, std::vector<ExpressionPtr>{longLit(1)});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, functionCallNotEqualDistinct) {
  auto name =
      std::make_shared<QualifiedName>(loc(), std::vector<std::string>{"f"});
  auto a = std::make_shared<FunctionCall>(
      loc(),
      name,
      nullptr,
      nullptr,
      nullptr,
      /*distinct=*/false,
      /*ignoreNulls=*/false,
      std::vector<ExpressionPtr>{longLit(1)});
  auto b = std::make_shared<FunctionCall>(
      loc(),
      name,
      nullptr,
      nullptr,
      nullptr,
      /*distinct=*/true,
      /*ignoreNulls=*/false,
      std::vector<ExpressionPtr>{longLit(1)});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, castEquals) {
  auto type1 = std::make_shared<TypeSignature>(loc(), "varchar");
  auto type2 = std::make_shared<TypeSignature>(loc(5, 5), "varchar");
  auto a = std::make_shared<Cast>(loc(0, 0), longLit(1), type1, false);
  auto b = std::make_shared<Cast>(loc(5, 5), longLit(1), type2, false);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, castNotEqualSafe) {
  auto type = std::make_shared<TypeSignature>(loc(), "varchar");
  auto a = std::make_shared<Cast>(loc(), longLit(1), type, false);
  auto b = std::make_shared<Cast>(loc(), longLit(1), type, true);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, extractEquals) {
  auto a =
      std::make_shared<Extract>(loc(0, 0), longLit(1), Extract::Field::kYear);
  auto b =
      std::make_shared<Extract>(loc(5, 5), longLit(1), Extract::Field::kYear);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, extractNotEqualField) {
  auto a = std::make_shared<Extract>(loc(), longLit(1), Extract::Field::kYear);
  auto b = std::make_shared<Extract>(loc(), longLit(1), Extract::Field::kMonth);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, currentTimeEquals) {
  auto a = std::make_shared<CurrentTime>(
      loc(0, 0), CurrentTime::Function::kTimestamp, 3);
  auto b = std::make_shared<CurrentTime>(
      loc(5, 5), CurrentTime::Function::kTimestamp, 3);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, currentTimeNotEqualFunction) {
  auto a =
      std::make_shared<CurrentTime>(loc(), CurrentTime::Function::kTimestamp);
  auto b = std::make_shared<CurrentTime>(loc(), CurrentTime::Function::kDate);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, currentTimeNotEqualPrecision) {
  auto a = std::make_shared<CurrentTime>(
      loc(), CurrentTime::Function::kTimestamp, 3);
  auto b = std::make_shared<CurrentTime>(
      loc(), CurrentTime::Function::kTimestamp, 6);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, currentUserAlwaysEqual) {
  auto a = std::make_shared<CurrentUser>(loc(0, 0));
  auto b = std::make_shared<CurrentUser>(loc(5, 5));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, atTimeZoneEquals) {
  auto tz1 = std::make_shared<StringLiteral>(loc(), "UTC");
  auto tz2 = std::make_shared<StringLiteral>(loc(), "UTC");
  auto a = std::make_shared<AtTimeZone>(loc(0, 0), longLit(1), tz1);
  auto b = std::make_shared<AtTimeZone>(loc(5, 5), longLit(1), tz2);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, atTimeZoneNotEqual) {
  auto utc = std::make_shared<StringLiteral>(loc(), "UTC");
  auto pst = std::make_shared<StringLiteral>(loc(), "PST");
  auto a = std::make_shared<AtTimeZone>(loc(), longLit(1), utc);
  auto b = std::make_shared<AtTimeZone>(loc(), longLit(1), pst);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, subqueryExpressionEqualsSharedInner) {
  // Two distinct SubqueryExpression instances wrapping the same inner Query.
  auto select = std::make_shared<Select>(
      loc(), false, std::vector<std::shared_ptr<SelectItem>>{});
  auto querySpec = std::make_shared<QuerySpecification>(loc(), select);
  StatementPtr query = std::make_shared<Query>(loc(), nullptr, querySpec);
  auto a = std::make_shared<SubqueryExpression>(loc(0, 0), query);
  auto b = std::make_shared<SubqueryExpression>(loc(5, 5), query);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, subqueryExpressionEqualsDistinctInner) {
  auto makeQuery = []() {
    auto select = std::make_shared<Select>(
        loc(), false, std::vector<std::shared_ptr<SelectItem>>{});
    auto querySpec = std::make_shared<QuerySpecification>(loc(), select);
    return std::static_pointer_cast<Statement>(
        std::make_shared<Query>(loc(), nullptr, querySpec));
  };
  auto a = std::make_shared<SubqueryExpression>(loc(0, 0), makeQuery());
  auto b = std::make_shared<SubqueryExpression>(loc(5, 5), makeQuery());
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, subqueryExpressionNotEqualNullVsNonNull) {
  auto select = std::make_shared<Select>(
      loc(), false, std::vector<std::shared_ptr<SelectItem>>{});
  auto querySpec = std::make_shared<QuerySpecification>(loc(), select);
  StatementPtr query = std::make_shared<Query>(loc(), nullptr, querySpec);
  auto a = std::make_shared<SubqueryExpression>(loc(), query);
  auto b = std::make_shared<SubqueryExpression>(loc(), nullptr);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, arrayConstructorEquals) {
  auto a = std::make_shared<ArrayConstructor>(
      loc(0, 0), std::vector<ExpressionPtr>{longLit(1), longLit(2)});
  auto b = std::make_shared<ArrayConstructor>(
      loc(5, 5), std::vector<ExpressionPtr>{longLit(1), longLit(2)});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, arrayConstructorNotEqual) {
  auto a = std::make_shared<ArrayConstructor>(
      loc(), std::vector<ExpressionPtr>{longLit(1)});
  auto b = std::make_shared<ArrayConstructor>(
      loc(), std::vector<ExpressionPtr>{longLit(2)});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, rowEquals) {
  auto a = std::make_shared<Row>(
      loc(0, 0), std::vector<ExpressionPtr>{longLit(1), longLit(2)});
  auto b = std::make_shared<Row>(
      loc(5, 5), std::vector<ExpressionPtr>{longLit(1), longLit(2)});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, rowNotEqual) {
  auto a = std::make_shared<Row>(loc(), std::vector<ExpressionPtr>{longLit(1)});
  auto b = std::make_shared<Row>(loc(), std::vector<ExpressionPtr>{longLit(2)});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, namedRowEquals) {
  auto a = std::make_shared<NamedRow>(
      loc(0, 0),
      std::vector<ExpressionPtr>{longLit(1)},
      std::vector<std::string>{"f"});
  auto b = std::make_shared<NamedRow>(
      loc(5, 5),
      std::vector<ExpressionPtr>{longLit(1)},
      std::vector<std::string>{"f"});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, namedRowNotEqualFieldName) {
  auto a = std::make_shared<NamedRow>(
      loc(),
      std::vector<ExpressionPtr>{longLit(1)},
      std::vector<std::string>{"f"});
  auto b = std::make_shared<NamedRow>(
      loc(),
      std::vector<ExpressionPtr>{longLit(1)},
      std::vector<std::string>{"g"});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, subscriptExpressionEquals) {
  auto a =
      std::make_shared<SubscriptExpression>(loc(0, 0), longLit(1), longLit(2));
  auto b =
      std::make_shared<SubscriptExpression>(loc(5, 5), longLit(1), longLit(2));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, subscriptExpressionNotEqual) {
  auto a = std::make_shared<SubscriptExpression>(loc(), longLit(1), longLit(2));
  auto b = std::make_shared<SubscriptExpression>(loc(), longLit(1), longLit(3));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, lambdaArgumentDeclarationEquals) {
  auto id1 = std::make_shared<Identifier>(loc(), "x", false);
  auto id2 = std::make_shared<Identifier>(loc(), "x", false);
  auto a = std::make_shared<LambdaArgumentDeclaration>(loc(0, 0), id1);
  auto b = std::make_shared<LambdaArgumentDeclaration>(loc(5, 5), id2);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, lambdaArgumentDeclarationNotEqual) {
  auto x = std::make_shared<Identifier>(loc(), "x", false);
  auto y = std::make_shared<Identifier>(loc(), "y", false);
  auto a = std::make_shared<LambdaArgumentDeclaration>(loc(), x);
  auto b = std::make_shared<LambdaArgumentDeclaration>(loc(), y);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, lambdaExpressionEquals) {
  auto id = std::make_shared<Identifier>(loc(), "x", false);
  auto arg1 = std::make_shared<LambdaArgumentDeclaration>(loc(), id);
  auto arg2 = std::make_shared<LambdaArgumentDeclaration>(loc(), id);
  auto a = std::make_shared<LambdaExpression>(
      loc(0, 0),
      std::vector<std::shared_ptr<LambdaArgumentDeclaration>>{arg1},
      longLit(1));
  auto b = std::make_shared<LambdaExpression>(
      loc(5, 5),
      std::vector<std::shared_ptr<LambdaArgumentDeclaration>>{arg2},
      longLit(1));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, lambdaExpressionNotEqualBody) {
  auto id = std::make_shared<Identifier>(loc(), "x", false);
  auto arg = std::make_shared<LambdaArgumentDeclaration>(loc(), id);
  auto a = std::make_shared<LambdaExpression>(
      loc(),
      std::vector<std::shared_ptr<LambdaArgumentDeclaration>>{arg},
      longLit(1));
  auto b = std::make_shared<LambdaExpression>(
      loc(),
      std::vector<std::shared_ptr<LambdaArgumentDeclaration>>{arg},
      longLit(2));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, bindExpressionEquals) {
  auto a = std::make_shared<BindExpression>(
      loc(0, 0), std::vector<ExpressionPtr>{longLit(1)}, longLit(2));
  auto b = std::make_shared<BindExpression>(
      loc(5, 5), std::vector<ExpressionPtr>{longLit(1)}, longLit(2));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, bindExpressionNotEqual) {
  auto a = std::make_shared<BindExpression>(
      loc(), std::vector<ExpressionPtr>{longLit(1)}, longLit(2));
  auto b = std::make_shared<BindExpression>(
      loc(), std::vector<ExpressionPtr>{longLit(1)}, longLit(3));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, groupingOperationEquals) {
  auto a = std::make_shared<GroupingOperation>(
      loc(0, 0),
      std::vector<ExpressionPtr>{
          std::make_shared<Identifier>(loc(), "a", false)});
  auto b = std::make_shared<GroupingOperation>(
      loc(5, 5),
      std::vector<ExpressionPtr>{
          std::make_shared<Identifier>(loc(), "a", false)});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, groupingOperationNotEqual) {
  auto a = std::make_shared<GroupingOperation>(
      loc(),
      std::vector<ExpressionPtr>{
          std::make_shared<Identifier>(loc(), "a", false)});
  auto b = std::make_shared<GroupingOperation>(
      loc(),
      std::vector<ExpressionPtr>{
          std::make_shared<Identifier>(loc(), "b", false)});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, tableVersionExpressionEquals) {
  auto a = std::make_shared<TableVersionExpression>(
      loc(0, 0),
      TableVersionExpression::TableVersionType::kVersion,
      longLit(1));
  auto b = std::make_shared<TableVersionExpression>(
      loc(5, 5),
      TableVersionExpression::TableVersionType::kVersion,
      longLit(1));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, tableVersionExpressionNotEqualType) {
  auto a = std::make_shared<TableVersionExpression>(
      loc(), TableVersionExpression::TableVersionType::kVersion, longLit(1));
  auto b = std::make_shared<TableVersionExpression>(
      loc(), TableVersionExpression::TableVersionType::kTimestamp, longLit(1));
  EXPECT_FALSE(*a == *b);
}

namespace {
std::shared_ptr<Select> emptySelect(bool distinct = false) {
  return std::make_shared<Select>(
      loc(), distinct, std::vector<std::shared_ptr<SelectItem>>{});
}

std::shared_ptr<QuerySpecification> makeQuerySpec(
    const std::shared_ptr<Select>& select,
    const RelationPtr& from = nullptr) {
  return std::make_shared<QuerySpecification>(loc(), select, from);
}

std::shared_ptr<Table> makeTable(const std::string& name) {
  auto qn =
      std::make_shared<QualifiedName>(loc(), std::vector<std::string>{name});
  return std::make_shared<Table>(loc(), qn);
}
} // namespace

TEST(AstEqualityTest, queryEquals) {
  auto a =
      std::make_shared<Query>(loc(0, 0), nullptr, makeQuerySpec(emptySelect()));
  auto b =
      std::make_shared<Query>(loc(5, 5), nullptr, makeQuerySpec(emptySelect()));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, queryNotEqualLimit) {
  auto a = std::make_shared<Query>(
      loc(),
      nullptr,
      makeQuerySpec(emptySelect()),
      nullptr,
      nullptr,
      std::string{"10"});
  auto b = std::make_shared<Query>(
      loc(),
      nullptr,
      makeQuerySpec(emptySelect()),
      nullptr,
      nullptr,
      std::string{"20"});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, querySpecificationEquals) {
  auto a = makeQuerySpec(emptySelect(), makeTable("t"));
  auto b = makeQuerySpec(emptySelect(), makeTable("t"));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, querySpecificationNotEqualFrom) {
  auto a = makeQuerySpec(emptySelect(), makeTable("t1"));
  auto b = makeQuerySpec(emptySelect(), makeTable("t2"));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, singleColumnEquals) {
  auto a = std::make_shared<SingleColumn>(loc(0, 0), longLit(1));
  auto b = std::make_shared<SingleColumn>(loc(5, 5), longLit(1));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, singleColumnNotEqualAlias) {
  auto aliasX = std::make_shared<Identifier>(loc(), "x", false);
  auto aliasY = std::make_shared<Identifier>(loc(), "y", false);
  auto a = std::make_shared<SingleColumn>(loc(), longLit(1), aliasX);
  auto b = std::make_shared<SingleColumn>(loc(), longLit(1), aliasY);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, allColumnsEquals) {
  auto a = std::make_shared<AllColumns>(
      loc(0, 0),
      nullptr,
      std::vector<std::shared_ptr<Identifier>>{},
      std::vector<ReplaceItem>{});
  auto b = std::make_shared<AllColumns>(
      loc(5, 5),
      nullptr,
      std::vector<std::shared_ptr<Identifier>>{},
      std::vector<ReplaceItem>{});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, allColumnsNotEqualExclude) {
  auto excludeA = std::make_shared<Identifier>(loc(), "a", false);
  auto excludeB = std::make_shared<Identifier>(loc(), "b", false);
  auto a = std::make_shared<AllColumns>(
      loc(),
      nullptr,
      std::vector<std::shared_ptr<Identifier>>{excludeA},
      std::vector<ReplaceItem>{});
  auto b = std::make_shared<AllColumns>(
      loc(),
      nullptr,
      std::vector<std::shared_ptr<Identifier>>{excludeB},
      std::vector<ReplaceItem>{});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, selectColumnsEquals) {
  auto a = std::make_shared<SelectColumns>(
      loc(0, 0),
      "a.*",
      nullptr,
      std::vector<std::shared_ptr<Identifier>>{},
      std::vector<ReplaceItem>{});
  auto b = std::make_shared<SelectColumns>(
      loc(5, 5),
      "a.*",
      nullptr,
      std::vector<std::shared_ptr<Identifier>>{},
      std::vector<ReplaceItem>{});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, selectColumnsNotEqualPattern) {
  auto a = std::make_shared<SelectColumns>(
      loc(),
      "a.*",
      nullptr,
      std::vector<std::shared_ptr<Identifier>>{},
      std::vector<ReplaceItem>{});
  auto b = std::make_shared<SelectColumns>(
      loc(),
      "b.*",
      nullptr,
      std::vector<std::shared_ptr<Identifier>>{},
      std::vector<ReplaceItem>{});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, selectEquals) {
  auto a = emptySelect();
  auto b = emptySelect();
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, selectNotEqualDistinct) {
  // SELECT a vs SELECT DISTINCT a — the distinct flag matters.
  auto item1 = std::make_shared<SingleColumn>(loc(), longLit(1));
  auto item2 = std::make_shared<SingleColumn>(loc(), longLit(1));
  auto a = std::make_shared<Select>(
      loc(), false, std::vector<std::shared_ptr<SelectItem>>{item1});
  auto b = std::make_shared<Select>(
      loc(), true, std::vector<std::shared_ptr<SelectItem>>{item2});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, tableEquals) {
  auto a = makeTable("t");
  auto b = makeTable("t");
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, tableNotEqualName) {
  auto a = makeTable("t1");
  auto b = makeTable("t2");
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, aliasedRelationEquals) {
  auto aliasA = std::make_shared<Identifier>(loc(), "x", false);
  auto aliasB = std::make_shared<Identifier>(loc(), "x", false);
  auto a = std::make_shared<AliasedRelation>(loc(0, 0), makeTable("t"), aliasA);
  auto b = std::make_shared<AliasedRelation>(loc(5, 5), makeTable("t"), aliasB);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, aliasedRelationNotEqualAlias) {
  // t1 AS x vs t1 AS y — alias name participates in equality.
  auto aliasX = std::make_shared<Identifier>(loc(), "x", false);
  auto aliasY = std::make_shared<Identifier>(loc(), "y", false);
  auto a = std::make_shared<AliasedRelation>(loc(), makeTable("t1"), aliasX);
  auto b = std::make_shared<AliasedRelation>(loc(), makeTable("t1"), aliasY);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, sampledRelationEquals) {
  auto a = std::make_shared<SampledRelation>(
      loc(0, 0),
      makeTable("t"),
      SampledRelation::Type::kBernoulli,
      longLit(10));
  auto b = std::make_shared<SampledRelation>(
      loc(5, 5),
      makeTable("t"),
      SampledRelation::Type::kBernoulli,
      longLit(10));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, sampledRelationNotEqualType) {
  auto a = std::make_shared<SampledRelation>(
      loc(), makeTable("t"), SampledRelation::Type::kBernoulli, longLit(10));
  auto b = std::make_shared<SampledRelation>(
      loc(), makeTable("t"), SampledRelation::Type::kSystem, longLit(10));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, tableSubqueryEquals) {
  auto makeQuery = []() {
    return std::static_pointer_cast<Statement>(
        std::make_shared<Query>(loc(), nullptr, makeQuerySpec(emptySelect())));
  };
  auto a = std::make_shared<TableSubquery>(loc(0, 0), makeQuery());
  auto b = std::make_shared<TableSubquery>(loc(5, 5), makeQuery());
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, tableSubqueryNotEqual) {
  auto qa = std::static_pointer_cast<Statement>(std::make_shared<Query>(
      loc(), nullptr, makeQuerySpec(emptySelect(), makeTable("t1"))));
  auto qb = std::static_pointer_cast<Statement>(std::make_shared<Query>(
      loc(), nullptr, makeQuerySpec(emptySelect(), makeTable("t2"))));
  auto a = std::make_shared<TableSubquery>(loc(), qa);
  auto b = std::make_shared<TableSubquery>(loc(), qb);
  EXPECT_FALSE(*a == *b);
}

std::shared_ptr<Statement> makeLateralQuery(const std::string& table) {
  return std::make_shared<Query>(
      loc(), nullptr, makeQuerySpec(emptySelect(), makeTable(table)));
}

TEST(AstEqualityTest, lateralEquals) {
  auto a = std::make_shared<Lateral>(loc(0, 0), makeLateralQuery("t"));
  auto b = std::make_shared<Lateral>(loc(5, 5), makeLateralQuery("t"));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, lateralNotEqual) {
  auto a = std::make_shared<Lateral>(loc(), makeLateralQuery("t1"));
  auto b = std::make_shared<Lateral>(loc(), makeLateralQuery("t2"));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, unnestEquals) {
  auto a = std::make_shared<Unnest>(
      loc(0, 0), std::vector<ExpressionPtr>{longLit(1)}, false);
  auto b = std::make_shared<Unnest>(
      loc(5, 5), std::vector<ExpressionPtr>{longLit(1)}, false);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, unnestNotEqualOrdinality) {
  // UNNEST(x) vs UNNEST(x) WITH ORDINALITY — the ordinality flag matters.
  auto a = std::make_shared<Unnest>(
      loc(), std::vector<ExpressionPtr>{longLit(1)}, false);
  auto b = std::make_shared<Unnest>(
      loc(), std::vector<ExpressionPtr>{longLit(1)}, true);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, valuesEquals) {
  auto a = std::make_shared<Values>(
      loc(0, 0), std::vector<ExpressionPtr>{longLit(1), longLit(2)});
  auto b = std::make_shared<Values>(
      loc(5, 5), std::vector<ExpressionPtr>{longLit(1), longLit(2)});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, valuesNotEqualRows) {
  auto a =
      std::make_shared<Values>(loc(), std::vector<ExpressionPtr>{longLit(1)});
  auto b =
      std::make_shared<Values>(loc(), std::vector<ExpressionPtr>{longLit(2)});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, joinOnEquals) {
  auto a = std::make_shared<JoinOn>(loc(0, 0), longLit(1));
  auto b = std::make_shared<JoinOn>(loc(5, 5), longLit(1));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, joinOnNotEqual) {
  auto a = std::make_shared<JoinOn>(loc(), longLit(1));
  auto b = std::make_shared<JoinOn>(loc(), longLit(2));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, joinUsingEquals) {
  auto colA = std::make_shared<Identifier>(loc(), "a", false);
  auto colB = std::make_shared<Identifier>(loc(), "a", false);
  auto a = std::make_shared<JoinUsing>(
      loc(0, 0), std::vector<std::shared_ptr<Identifier>>{colA});
  auto b = std::make_shared<JoinUsing>(
      loc(5, 5), std::vector<std::shared_ptr<Identifier>>{colB});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, joinUsingNotEqual) {
  auto colA = std::make_shared<Identifier>(loc(), "a", false);
  auto colB = std::make_shared<Identifier>(loc(), "b", false);
  auto a = std::make_shared<JoinUsing>(
      loc(), std::vector<std::shared_ptr<Identifier>>{colA});
  auto b = std::make_shared<JoinUsing>(
      loc(), std::vector<std::shared_ptr<Identifier>>{colB});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, joinCriteriaDifferentSubtypes) {
  // JoinOn vs JoinUsing — different concrete JoinCriteria types are unequal
  // via the base Node::operator== type_ check.
  auto onCrit = std::make_shared<JoinOn>(loc(), longLit(1));
  auto col = std::make_shared<Identifier>(loc(), "a", false);
  auto usingCrit = std::make_shared<JoinUsing>(
      loc(), std::vector<std::shared_ptr<Identifier>>{col});
  EXPECT_FALSE(
      static_cast<const Node&>(*onCrit) ==
      static_cast<const Node&>(*usingCrit));
}

TEST(AstEqualityTest, joinEquals) {
  auto critA = std::make_shared<JoinOn>(loc(), longLit(1));
  auto critB = std::make_shared<JoinOn>(loc(), longLit(1));
  auto a = std::make_shared<Join>(
      loc(0, 0), Join::Type::kInner, makeTable("t1"), makeTable("t2"), critA);
  auto b = std::make_shared<Join>(
      loc(5, 5), Join::Type::kInner, makeTable("t1"), makeTable("t2"), critB);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, joinNotEqualType) {
  // INNER JOIN vs LEFT JOIN — the join type matters.
  auto crit = std::make_shared<JoinOn>(loc(), longLit(1));
  auto a = std::make_shared<Join>(
      loc(), Join::Type::kInner, makeTable("t1"), makeTable("t2"), crit);
  auto b = std::make_shared<Join>(
      loc(), Join::Type::kLeft, makeTable("t1"), makeTable("t2"), crit);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, naturalJoinEquals) {
  auto a = std::make_shared<NaturalJoin>(
      loc(0, 0), Join::Type::kInner, makeTable("t1"), makeTable("t2"));
  auto b = std::make_shared<NaturalJoin>(
      loc(5, 5), Join::Type::kInner, makeTable("t1"), makeTable("t2"));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, naturalJoinNotEqualType) {
  auto a = std::make_shared<NaturalJoin>(
      loc(), Join::Type::kInner, makeTable("t1"), makeTable("t2"));
  auto b = std::make_shared<NaturalJoin>(
      loc(), Join::Type::kLeft, makeTable("t1"), makeTable("t2"));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, unionEquals) {
  auto a = std::make_shared<Union>(
      loc(0, 0), makeTable("t1"), makeTable("t2"), true);
  auto b = std::make_shared<Union>(
      loc(5, 5), makeTable("t1"), makeTable("t2"), true);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, unionNotEqualDistinct) {
  // UNION (distinct=true) vs UNION ALL (distinct=false).
  auto a =
      std::make_shared<Union>(loc(), makeTable("t1"), makeTable("t2"), true);
  auto b =
      std::make_shared<Union>(loc(), makeTable("t1"), makeTable("t2"), false);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, intersectEquals) {
  auto a = std::make_shared<Intersect>(
      loc(0, 0), makeTable("t1"), makeTable("t2"), true);
  auto b = std::make_shared<Intersect>(
      loc(5, 5), makeTable("t1"), makeTable("t2"), true);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, intersectNotEqualOperand) {
  auto a = std::make_shared<Intersect>(
      loc(), makeTable("t1"), makeTable("t2"), true);
  auto b = std::make_shared<Intersect>(
      loc(), makeTable("t1"), makeTable("t3"), true);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, exceptEquals) {
  auto a = std::make_shared<Except>(
      loc(0, 0), makeTable("t1"), makeTable("t2"), true);
  auto b = std::make_shared<Except>(
      loc(5, 5), makeTable("t1"), makeTable("t2"), true);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, exceptNotEqualDistinct) {
  auto a =
      std::make_shared<Except>(loc(), makeTable("t1"), makeTable("t2"), true);
  auto b =
      std::make_shared<Except>(loc(), makeTable("t1"), makeTable("t2"), false);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, typeSignatureEquals) {
  auto a = std::make_shared<TypeSignature>(loc(0, 0), "varchar");
  auto b = std::make_shared<TypeSignature>(loc(5, 5), "varchar");
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, typeSignatureNotEqualBaseName) {
  auto a = std::make_shared<TypeSignature>(loc(), "varchar");
  auto b = std::make_shared<TypeSignature>(loc(), "bigint");
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, typeSignatureWithParameters) {
  auto k1 = std::make_shared<TypeSignature>(loc(), "K");
  auto v1 = std::make_shared<TypeSignature>(loc(), "V");
  auto k2 = std::make_shared<TypeSignature>(loc(), "K");
  auto v2 = std::make_shared<TypeSignature>(loc(), "V");
  auto a = std::make_shared<TypeSignature>(
      loc(), "map", std::vector<TypeSignaturePtr>{k1, v1});
  auto b = std::make_shared<TypeSignature>(
      loc(), "map", std::vector<TypeSignaturePtr>{k2, v2});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, callArgumentEquals) {
  auto name1 = std::make_shared<Identifier>(loc(), "x", false);
  auto name2 = std::make_shared<Identifier>(loc(), "x", false);
  auto a = std::make_shared<CallArgument>(loc(0, 0), name1, longLit(1));
  auto b = std::make_shared<CallArgument>(loc(5, 5), name2, longLit(1));
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, callArgumentNotEqualValue) {
  auto name = std::make_shared<Identifier>(loc(), "x", false);
  auto a = std::make_shared<CallArgument>(loc(), name, longLit(1));
  auto b = std::make_shared<CallArgument>(loc(), name, longLit(2));
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, frameBoundEquals) {
  auto a = std::make_shared<FrameBound>(
      loc(0, 0), FrameBound::Type::kUnboundedPreceding);
  auto b = std::make_shared<FrameBound>(
      loc(5, 5), FrameBound::Type::kUnboundedPreceding);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, frameBoundNotEqualType) {
  auto a = std::make_shared<FrameBound>(
      loc(), FrameBound::Type::kUnboundedPreceding);
  auto b = std::make_shared<FrameBound>(loc(), FrameBound::Type::kCurrentRow);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, windowFrameEquals) {
  auto start1 = std::make_shared<FrameBound>(
      loc(), FrameBound::Type::kUnboundedPreceding);
  auto start2 = std::make_shared<FrameBound>(
      loc(), FrameBound::Type::kUnboundedPreceding);
  auto a = std::make_shared<WindowFrame>(
      loc(0, 0), WindowFrame::Type::kRows, start1);
  auto b = std::make_shared<WindowFrame>(
      loc(5, 5), WindowFrame::Type::kRows, start2);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, windowFrameNotEqualType) {
  auto start1 = std::make_shared<FrameBound>(
      loc(), FrameBound::Type::kUnboundedPreceding);
  auto start2 = std::make_shared<FrameBound>(
      loc(), FrameBound::Type::kUnboundedPreceding);
  auto a =
      std::make_shared<WindowFrame>(loc(), WindowFrame::Type::kRows, start1);
  auto b =
      std::make_shared<WindowFrame>(loc(), WindowFrame::Type::kRange, start2);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, windowEquals) {
  auto a = std::make_shared<Window>(
      loc(0, 0), std::vector<ExpressionPtr>{longLit(1)});
  auto b = std::make_shared<Window>(
      loc(5, 5), std::vector<ExpressionPtr>{longLit(1)});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, windowNotEqualPartitionBy) {
  auto a =
      std::make_shared<Window>(loc(), std::vector<ExpressionPtr>{longLit(1)});
  auto b =
      std::make_shared<Window>(loc(), std::vector<ExpressionPtr>{longLit(2)});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, sortItemEquals) {
  auto a = std::make_shared<SortItem>(
      loc(0, 0),
      longLit(1),
      SortItem::Ordering::kAscending,
      SortItem::NullOrdering::kFirst);
  auto b = std::make_shared<SortItem>(
      loc(5, 5),
      longLit(1),
      SortItem::Ordering::kAscending,
      SortItem::NullOrdering::kFirst);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, sortItemNotEqualOrdering) {
  auto a = std::make_shared<SortItem>(
      loc(),
      longLit(1),
      SortItem::Ordering::kAscending,
      SortItem::NullOrdering::kFirst);
  auto b = std::make_shared<SortItem>(
      loc(),
      longLit(1),
      SortItem::Ordering::kDescending,
      SortItem::NullOrdering::kFirst);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, sortItemNotEqualNullOrdering) {
  auto a = std::make_shared<SortItem>(
      loc(),
      longLit(1),
      SortItem::Ordering::kAscending,
      SortItem::NullOrdering::kFirst);
  auto b = std::make_shared<SortItem>(
      loc(),
      longLit(1),
      SortItem::Ordering::kAscending,
      SortItem::NullOrdering::kLast);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, orderByEquals) {
  auto item1 = std::make_shared<SortItem>(
      loc(),
      longLit(1),
      SortItem::Ordering::kAscending,
      SortItem::NullOrdering::kFirst);
  auto item2 = std::make_shared<SortItem>(
      loc(),
      longLit(1),
      SortItem::Ordering::kAscending,
      SortItem::NullOrdering::kFirst);
  auto a = std::make_shared<OrderBy>(
      loc(0, 0), std::vector<std::shared_ptr<SortItem>>{item1});
  auto b = std::make_shared<OrderBy>(
      loc(5, 5), std::vector<std::shared_ptr<SortItem>>{item2});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, orderByNotEqual) {
  auto itemAsc = std::make_shared<SortItem>(
      loc(),
      longLit(1),
      SortItem::Ordering::kAscending,
      SortItem::NullOrdering::kFirst);
  auto itemDesc = std::make_shared<SortItem>(
      loc(),
      longLit(1),
      SortItem::Ordering::kDescending,
      SortItem::NullOrdering::kFirst);
  auto a = std::make_shared<OrderBy>(
      loc(), std::vector<std::shared_ptr<SortItem>>{itemAsc});
  auto b = std::make_shared<OrderBy>(
      loc(), std::vector<std::shared_ptr<SortItem>>{itemDesc});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, offsetEquals) {
  auto a = std::make_shared<Offset>(loc(0, 0), "10");
  auto b = std::make_shared<Offset>(loc(5, 5), "10");
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, offsetNotEqual) {
  auto a = std::make_shared<Offset>(loc(), "10");
  auto b = std::make_shared<Offset>(loc(), "20");
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, simpleGroupByEquals) {
  auto a = std::make_shared<SimpleGroupBy>(
      loc(0, 0), std::vector<ExpressionPtr>{longLit(1)});
  auto b = std::make_shared<SimpleGroupBy>(
      loc(5, 5), std::vector<ExpressionPtr>{longLit(1)});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, simpleGroupByNotEqual) {
  auto a = std::make_shared<SimpleGroupBy>(
      loc(), std::vector<ExpressionPtr>{longLit(1)});
  auto b = std::make_shared<SimpleGroupBy>(
      loc(), std::vector<ExpressionPtr>{longLit(2)});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, groupingSetsEquals) {
  auto a = std::make_shared<GroupingSets>(
      loc(0, 0),
      std::vector<std::vector<ExpressionPtr>>{{longLit(1)}, {longLit(2)}});
  auto b = std::make_shared<GroupingSets>(
      loc(5, 5),
      std::vector<std::vector<ExpressionPtr>>{{longLit(1)}, {longLit(2)}});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, groupingSetsNotEqual) {
  auto a = std::make_shared<GroupingSets>(
      loc(), std::vector<std::vector<ExpressionPtr>>{{longLit(1)}});
  auto b = std::make_shared<GroupingSets>(
      loc(), std::vector<std::vector<ExpressionPtr>>{{longLit(2)}});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, cubeEquals) {
  auto a =
      std::make_shared<Cube>(loc(0, 0), std::vector<ExpressionPtr>{longLit(1)});
  auto b =
      std::make_shared<Cube>(loc(5, 5), std::vector<ExpressionPtr>{longLit(1)});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, cubeNotEqual) {
  auto a =
      std::make_shared<Cube>(loc(), std::vector<ExpressionPtr>{longLit(1)});
  auto b =
      std::make_shared<Cube>(loc(), std::vector<ExpressionPtr>{longLit(2)});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, rollupEquals) {
  auto a = std::make_shared<Rollup>(
      loc(0, 0), std::vector<ExpressionPtr>{longLit(1)});
  auto b = std::make_shared<Rollup>(
      loc(5, 5), std::vector<ExpressionPtr>{longLit(1)});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, rollupNotEqual) {
  auto a =
      std::make_shared<Rollup>(loc(), std::vector<ExpressionPtr>{longLit(1)});
  auto b =
      std::make_shared<Rollup>(loc(), std::vector<ExpressionPtr>{longLit(2)});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, groupByEquals) {
  auto elem1 = std::make_shared<SimpleGroupBy>(
      loc(), std::vector<ExpressionPtr>{longLit(1)});
  auto elem2 = std::make_shared<SimpleGroupBy>(
      loc(), std::vector<ExpressionPtr>{longLit(1)});
  auto a = std::make_shared<GroupBy>(
      loc(0, 0), false, std::vector<GroupingElementPtr>{elem1});
  auto b = std::make_shared<GroupBy>(
      loc(5, 5), false, std::vector<GroupingElementPtr>{elem2});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, groupByNotEqualDistinct) {
  auto elem = std::make_shared<SimpleGroupBy>(
      loc(), std::vector<ExpressionPtr>{longLit(1)});
  auto a = std::make_shared<GroupBy>(
      loc(), false, std::vector<GroupingElementPtr>{elem});
  auto b = std::make_shared<GroupBy>(
      loc(), true, std::vector<GroupingElementPtr>{elem});
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, withQueryEquals) {
  auto name1 = std::make_shared<Identifier>(loc(), "cte", false);
  auto name2 = std::make_shared<Identifier>(loc(), "cte", false);
  auto query1 =
      std::make_shared<Query>(loc(), nullptr, makeQuerySpec(emptySelect()));
  auto query2 =
      std::make_shared<Query>(loc(), nullptr, makeQuerySpec(emptySelect()));
  auto a = std::make_shared<WithQuery>(loc(0, 0), name1, query1);
  auto b = std::make_shared<WithQuery>(loc(5, 5), name2, query2);
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, withQueryNotEqualName) {
  auto nameA = std::make_shared<Identifier>(loc(), "a", false);
  auto nameB = std::make_shared<Identifier>(loc(), "b", false);
  auto query =
      std::make_shared<Query>(loc(), nullptr, makeQuerySpec(emptySelect()));
  auto a = std::make_shared<WithQuery>(loc(), nameA, query);
  auto b = std::make_shared<WithQuery>(loc(), nameB, query);
  EXPECT_FALSE(*a == *b);
}

TEST(AstEqualityTest, withEquals) {
  auto name1 = std::make_shared<Identifier>(loc(), "cte", false);
  auto name2 = std::make_shared<Identifier>(loc(), "cte", false);
  auto query1 =
      std::make_shared<Query>(loc(), nullptr, makeQuerySpec(emptySelect()));
  auto query2 =
      std::make_shared<Query>(loc(), nullptr, makeQuerySpec(emptySelect()));
  auto wq1 = std::make_shared<WithQuery>(loc(), name1, query1);
  auto wq2 = std::make_shared<WithQuery>(loc(), name2, query2);
  auto a = std::make_shared<With>(
      loc(0, 0), false, std::vector<std::shared_ptr<WithQuery>>{wq1});
  auto b = std::make_shared<With>(
      loc(5, 5), false, std::vector<std::shared_ptr<WithQuery>>{wq2});
  EXPECT_TRUE(*a == *b);
  EXPECT_EQ(a->hash(), b->hash());
}

TEST(AstEqualityTest, withNotEqualRecursive) {
  auto name = std::make_shared<Identifier>(loc(), "cte", false);
  auto query =
      std::make_shared<Query>(loc(), nullptr, makeQuerySpec(emptySelect()));
  auto wq = std::make_shared<WithQuery>(loc(), name, query);
  auto a = std::make_shared<With>(
      loc(), false, std::vector<std::shared_ptr<WithQuery>>{wq});
  auto b = std::make_shared<With>(
      loc(), true, std::vector<std::shared_ptr<WithQuery>>{wq});
  EXPECT_FALSE(*a == *b);
}

} // namespace
} // namespace axiom::sql::presto
