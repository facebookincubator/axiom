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

#include "axiom/cli/JsonPrinter.h"
#include <folly/json.h>
#include <iostream>

using namespace facebook::velox;

namespace axiom::sql {

namespace {

// Converts a Variant to a folly::dynamic for JSON serialization.
//
// We use a hybrid approach instead of directly using Variant::toJson() because
// Variant serializes ROW types as arrays (e.g., ["value1", "value2"]), but we
// want a standard JSON format (e.g., {"field1": "value1", "field2":
// "value2"}) for better usability and compatibility with downstream tools.
//
// The hybrid approach leverages variantAt() for efficient value extraction
// but has more complex logic for complex types
folly::dynamic variantToDynamic(const Variant& variant, const TypePtr& type) {
  if (variant.isNull()) {
    return nullptr;
  }

  switch (type->kind()) {
    case TypeKind::ROW: {
      // ROW types must be serialized as objects with named fields, not arrays.
      // This preserves field names in the JSON output for self-documentation.
      folly::dynamic obj = folly::dynamic::object;
      const auto& rowType = type->asRow();
      const auto& names = rowType.names();
      const auto& rowValue = variant.row();
      for (auto i = 0; i < names.size(); ++i) {
        obj[names[i]] = variantToDynamic(rowValue[i], rowType.childAt(i));
      }
      return obj;
    }
    case TypeKind::ARRAY: {
      folly::dynamic arr = folly::dynamic::array;
      const auto& arrayValue = variant.array();
      const auto& elementType = type->asArray().elementType();
      for (const auto& element : arrayValue) {
        arr.push_back(variantToDynamic(element, elementType));
      }
      return arr;
    }
    case TypeKind::MAP: {
      folly::dynamic obj = folly::dynamic::object;
      const auto& mapValue = variant.map();
      for (const auto& [key, value] : mapValue) {
        // Convert key to string for JSON object key.
        // JSON objects require string keys, so we convert non-string types.
        std::string keyStr;
        if (key.kind() == TypeKind::VARCHAR ||
            key.kind() == TypeKind::VARBINARY) {
          keyStr = key.value<std::string>();
        } else {
          // For non-string keys (e.g., integers), serialize to JSON string.
          keyStr = key.toJson(type->asMap().keyType());
        }
        obj[keyStr] = variantToDynamic(value, type->asMap().valueType());
      }
      return obj;
    }
    default:
      // For scalar types, Variant::toJson() produces the correct format.
      return folly::parseJson(variant.toJson(type));
  }
}

} // namespace

int32_t JsonPrinter::printResults(
    const std::vector<RowVectorPtr>& results,
    int32_t maxRows) {
  int32_t numPrinted = 0;
  int32_t totalRows = 0;

  for (const auto& result : results) {
    totalRows += result->size();

    if (result->size() == 0) {
      continue;
    }

    const auto& rowType = result->rowType();
    const auto& columnNames = rowType->names();
    const auto numColumns = columnNames.size();

    for (vector_size_t row = 0; row < result->size(); ++row) {
      if (numPrinted >= maxRows) {
        return totalRows;
      }

      folly::dynamic jsonObj = folly::dynamic::object;

      for (auto col = 0; col < numColumns; ++col) {
        const auto& columnName = columnNames[col];
        const auto& columnVector = result->childAt(col);
        const auto& columnType = rowType->childAt(col);

        auto variant = columnVector->variantAt(row);
        jsonObj[columnName] = variantToDynamic(variant, columnType);
      }

      std::cout << folly::toJson(jsonObj) << std::endl;
      ++numPrinted;
    }
  }

  return totalRows;
}

} // namespace axiom::sql
