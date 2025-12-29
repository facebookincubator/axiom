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
#include "axiom/optimizer/tests/ExprPrinters.h"
#include <algorithm>
#include <sstream>
#include <unordered_set>
#include "velox/common/Casts.h"
#include "velox/common/encode/Base64.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/VectorSaver.h"

namespace facebook::velox::core {

/// Escapes a name for use in identifier strings.
std::string escapeName(const std::string& name) {
  return folly::cEscape<std::string>(name);
}

/// Checks if a function name is a SQL reserved word that needs escaping.
bool isSqlReservedWord(const std::string& name) {
  // Convert to lowercase for case-insensitive comparison
  std::string lowerName = name;
  std::transform(
      lowerName.begin(), lowerName.end(), lowerName.begin(), ::tolower);

  // Common SQL reserved words that might appear as function names
  static const std::unordered_set<std::string> reservedWords = {
      // Logical operators
      "and",
      "or",
      "not",
      // Comparison and set operators
      "in",
      "is",
      "like",
      "between",
      "exists",
      "all",
      "any",
      "some",
      // Control flow
      "case",
      "when",
      "then",
      "else",
      "end",
      // DML keywords
      "select",
      "from",
      "where",
      "having",
      "group",
      "order",
      "by",
      "as",
      "on",
      // Join types
      "join",
      "inner",
      "outer",
      "left",
      "right",
      "full",
      "cross",
      // Set operations
      "union",
      "intersect",
      "except",
      // Data modification
      "insert",
      "update",
      "delete",
      // DDL
      "create",
      "drop",
      "alter",
      "table",
      // Literals and constants
      "null",
      "true",
      "false",
      // Other common keywords
      "distinct",
      "limit",
      "offset",
      "with",
      "recursive",
  };

  return reservedWords.find(lowerName) != reservedWords.end();
}

/// Checks if an identifier (field name or alias) needs quoting.
/// Returns true if the name is a SQL reserved word or contains special
/// characters like dots.
bool needsIdentifierQuoting(const std::string& name) {
  // Quote if it's a SQL reserved word
  if (isSqlReservedWord(name)) {
    return true;
  }

  // Quote if it contains a dot or other special characters
  for (char c : name) {
    if (c == '.' || c == ' ' || c == '-' || c == '*' || c == '/' ||
        c == '+' || c == '=' || c == '<' || c == '>' || c == '!' ||
        c == '(' || c == ')' || c == '[' || c == ']' || c == '{' ||
        c == '}' || c == ',' || c == ';' || c == ':') {
      return true;
    }
  }

  return false;
}

namespace {

/// Escapes a string for SQL representation.
/// Single quotes are doubled, and other SQL special characters are handled.
std::string escapeSqlString(const std::string& str) {
  std::string result;
  result.reserve(str.size() * 2); // Reserve space for potential escaping

  for (char c : str) {
    if (c == '\'') {
      // Single quote becomes two single quotes
      result += "''";
    } else if (c == '\\') {
      // Backslash becomes double backslash
      result += "\\\\";
    } else if (c == '\n') {
      // Newline
      result += "\\n";
    } else if (c == '\r') {
      // Carriage return
      result += "\\r";
    } else if (c == '\t') {
      // Tab
      result += "\\t";
    } else if (c == '\0') {
      // Null character
      result += "\\0";
    } else {
      result += c;
    }
  }

  return result;
}

std::string appendAliasIfExists(const std::string& name, const IExpr& expr) {
  if (!expr.alias().has_value()) {
    return name;
  }

  const std::string& alias = expr.alias().value();
  if (needsIdentifierQuoting(alias)) {
    return fmt::format("{} AS \"{}\"", name, alias);
  } else {
    return fmt::format("{} AS {}", name, alias);
  }
}

} // namespace

// ITypedExprPrinter implementations

std::string ITypedExprPrinter::toText(const ITypedExpr& expr) {
  switch (expr.kind()) {
    case ExprKind::kInput:
      return toTextImpl(*expr.asUnchecked<InputTypedExpr>());
    case ExprKind::kConstant:
      return toTextImpl(*expr.asUnchecked<ConstantTypedExpr>());
    case ExprKind::kCall:
      return toTextImpl(*expr.asUnchecked<CallTypedExpr>());
    case ExprKind::kFieldAccess:
      return toTextImpl(*expr.asUnchecked<FieldAccessTypedExpr>());
    case ExprKind::kDereference:
      return toTextImpl(*expr.asUnchecked<DereferenceTypedExpr>());
    case ExprKind::kConcat:
      return toTextImpl(*expr.asUnchecked<ConcatTypedExpr>());
    case ExprKind::kLambda:
      return toTextImpl(*expr.asUnchecked<LambdaTypedExpr>());
    case ExprKind::kCast:
      return toTextImpl(*expr.asUnchecked<CastTypedExpr>());
  }

  VELOX_UNREACHABLE();
}

std::string ITypedExprPrinter::toTextImpl(const InputTypedExpr& expr) {
  return "ROW";
}

std::string ITypedExprPrinter::toTextImpl(const ConstantTypedExpr& expr) {
  // Check if the constant is null
  if (expr.isNull()) {
    return "null";
  }

  // Special handling for VARCHAR type
  if (expr.type()->kind() == TypeKind::VARCHAR) {
    std::string strValue;

    if (expr.hasValueVector()) {
      strValue = expr.valueVector()->toString(0);
    } else {
      strValue = expr.value().toStringAsVector(expr.type());
    }

    // Apply SQL escaping and wrap in single quotes
    return "'" + escapeSqlString(strValue) + "'";
  }

  // Special handling for ARRAY type
  if (expr.type()->kind() == TypeKind::ARRAY) {
    auto elementType = expr.type()->childAt(0);
    std::string result = "array[";

    if (expr.hasValueVector()) {
      // Handle array from vector
      auto* wrappedVector = expr.valueVector()->wrappedVector();
      auto* arrayVector = wrappedVector->asUnchecked<ArrayVector>();
      auto wrappedIndex = expr.valueVector()->wrappedIndex(0);
      auto offset = arrayVector->offsetAt(wrappedIndex);
      auto size = arrayVector->sizeAt(wrappedIndex);

      // Check for empty array
      if (size == 0) {
        return "array[]";
      }

      auto elementsVector = arrayVector->elements();

      for (auto i = 0; i < size; ++i) {
        if (i > 0) {
          result += ", ";
        }

        // Check if element is null
        if (elementsVector->isNullAt(offset + i)) {
          result += "null";
        } else {
          // Create a constant expression for each element and call toText
          // recursively
          auto elementConstVector =
              BaseVector::wrapInConstant(1, offset + i, elementsVector);
          auto elementExpr = ConstantTypedExpr(elementConstVector);
          result += toTextImpl(elementExpr);
        }
      }
    } else {
      // Handle array from variant
      const auto& arrayValue = expr.value().value<TypeKind::ARRAY>();

      // Check for empty array
      if (arrayValue.size() == 0) {
        return "array[]";
      }

      for (size_t i = 0; i < arrayValue.size(); ++i) {
        if (i > 0) {
          result += ", ";
        }

        // Check if element is null
        if (arrayValue.at(i).isNull()) {
          result += "null";
        } else {
          // Create a constant expression for each element and call toText
          // recursively
          auto elementExpr = ConstantTypedExpr(elementType, arrayValue.at(i));
          result += toTextImpl(elementExpr);
        }
      }
    }

    result += "]";
    return result;
  }

  // Default behavior for non-VARCHAR types
  if (expr.hasValueVector()) {
    return expr.valueVector()->toString(0);
  }

  return expr.value().toStringAsVector(expr.type());
}

std::string ITypedExprPrinter::toTextImpl(const CallTypedExpr& expr) {
  std::string str{};

  // Escape function name if it's a SQL reserved word
  if (isSqlReservedWord(expr.name())) {
    str += "\"" + expr.name() + "\"";
  } else {
    str += expr.name();
  }

  str += "(";
  for (size_t i = 0; i < expr.inputs().size(); ++i) {
    auto& input = expr.inputs().at(i);
    if (i != 0) {
      str += ",";
    }
    str += toText(*input);
  }
  str += ")";
  return str;
}

std::string ITypedExprPrinter::toTextImpl(const FieldAccessTypedExpr& expr) {
  std::string fieldName;
  if (needsIdentifierQuoting(expr.name())) {
    fieldName = fmt::format("\"{}\"", escapeName(expr.name()));
  } else {
    fieldName = expr.name();
  }

  if (expr.inputs().empty()) {
    return fieldName;
  }

  return fmt::format("{}[{}]", toText(*expr.inputs()[0]), fieldName);
}

std::string ITypedExprPrinter::toTextImpl(const DereferenceTypedExpr& expr) {
  return fmt::format("{}[{}]", toText(*expr.inputs()[0]), expr.name());
}

std::string ITypedExprPrinter::toTextImpl(const ConcatTypedExpr& expr) {
  std::string str{};
  str += "CONCAT(";
  for (size_t i = 0; i < expr.inputs().size(); ++i) {
    auto& input = expr.inputs().at(i);
    if (i != 0) {
      str += ",";
    }
    str += toText(*input);
  }
  str += ")";
  return str;
}

std::string ITypedExprPrinter::toTextImpl(const LambdaTypedExpr& expr) {
  return fmt::format(
      "lambda {} -> {}", expr.signature()->toString(), toText(*expr.body()));
}

std::string ITypedExprPrinter::toTextImpl(const CastTypedExpr& expr) {
  if (expr.isTryCast()) {
    return fmt::format(
        "try_cast({} as {})",
        toText(*expr.inputs()[0]),
        expr.type()->toString());
  } else {
    return fmt::format(
        "cast({} as {})", toText(*expr.inputs()[0]), expr.type()->toString());
  }
}

// IExprPrinter implementations

std::string IExprPrinter::toText(const IExpr& expr) {
  if (auto* inputExpr = dynamic_cast<const InputExpr*>(&expr)) {
    return toTextImpl(*inputExpr);
  } else if (
      auto* fieldAccessExpr = dynamic_cast<const FieldAccessExpr*>(&expr)) {
    return toTextImpl(*fieldAccessExpr);
  } else if (auto* callExpr = dynamic_cast<const CallExpr*>(&expr)) {
    return toTextImpl(*callExpr);
  } else if (auto* constantExpr = dynamic_cast<const ConstantExpr*>(&expr)) {
    return toTextImpl(*constantExpr);
  } else if (auto* castExpr = dynamic_cast<const CastExpr*>(&expr)) {
    return toTextImpl(*castExpr);
  } else if (auto* lambdaExpr = dynamic_cast<const LambdaExpr*>(&expr)) {
    return toTextImpl(*lambdaExpr);
  }

  VELOX_UNREACHABLE();
}

std::string IExprPrinter::toTextImpl(const InputExpr& expr) {
  return "ROW";
}

std::string IExprPrinter::toTextImpl(const FieldAccessExpr& expr) {
  std::string fieldName;
  if (needsIdentifierQuoting(expr.name())) {
    fieldName = fmt::format("\"{}\"", escapeName(expr.name()));
  } else {
    fieldName = expr.name();
  }

  if (expr.isRootColumn()) {
    return appendAliasIfExists(fieldName, expr);
  }

  return appendAliasIfExists(
      fmt::format("dot({},{})", toText(*expr.input()), fieldName), expr);
}

std::string IExprPrinter::toTextImpl(const CallExpr& expr) {
  std::string buf;

  // Escape function name if it's a SQL reserved word
  if (isSqlReservedWord(expr.name())) {
    buf = "\"" + expr.name() + "\"(";
  } else {
    buf = expr.name() + "(";
  }

  bool first = true;
  for (auto& f : expr.inputs()) {
    if (!first) {
      buf += ",";
    }
    buf += toText(*f);
    first = false;
  }
  buf += ")";
  return appendAliasIfExists(buf, expr);
}

std::string IExprPrinter::toTextImpl(const ConstantExpr& expr) {
  // Check if the constant is null
  if (expr.value().isNull()) {
    return appendAliasIfExists("null", expr);
  }

  // Special handling for VARCHAR type
  if (expr.value().kind() == TypeKind::VARCHAR) {
    std::string strValue = expr.value().toStringAsVector(expr.type());
    // Apply SQL escaping, wrap in single quotes, and then add alias
    return appendAliasIfExists("'" + escapeSqlString(strValue) + "'", expr);
  }

  // Special handling for ARRAY type
  if (expr.value().kind() == TypeKind::ARRAY) {
    const auto& arrayValue = expr.value().value<TypeKind::ARRAY>();
    auto elementType = expr.type()->childAt(0);

    // Check for empty array
    if (arrayValue.size() == 0) {
      return appendAliasIfExists("array[]", expr);
    }

    std::string result = "array[";

    for (size_t i = 0; i < arrayValue.size(); ++i) {
      if (i > 0) {
        result += ", ";
      }

      // Check if element is null
      if (arrayValue.at(i).isNull()) {
        result += "null";
      } else {
        // Create a constant expression for each element and call toText
        // recursively
        auto elementExpr =
            ConstantExpr(elementType, arrayValue.at(i), std::nullopt);
        result += toText(elementExpr);
      }
    }

    result += "]";
    return appendAliasIfExists(result, expr);
  }

  // Default behavior for non-VARCHAR types
  return appendAliasIfExists(expr.value().toStringAsVector(expr.type()), expr);
}

std::string IExprPrinter::toTextImpl(const CastExpr& expr) {
  return appendAliasIfExists(
      "cast(" + toText(*expr.input()) + " as " + expr.type()->toString() + ")",
      expr);
}

std::string IExprPrinter::toTextImpl(const LambdaExpr& expr) {
  std::ostringstream out;

  if (expr.arguments().size() > 1) {
    out << "(" << folly::join(", ", expr.arguments()) << ")";
  } else {
    out << expr.arguments()[0];
  }
  out << " -> " << toText(*expr.body());
  return out.str();
}

} // namespace facebook::velox::core
