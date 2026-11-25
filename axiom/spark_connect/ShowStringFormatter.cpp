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

#include "axiom/spark_connect/ShowStringFormatter.h"

#include <algorithm>
#include <array>
#include <utility>

#include <fmt/format.h>

#include "velox/common/base/Exceptions.h"
#include "velox/external/utf8proc/utf8procImpl.h"
#include "velox/vector/DecodedVector.h"

namespace axiom::spark_connect {
namespace {

using facebook::velox::DecodedVector;
using facebook::velox::SelectivityVector;
using facebook::velox::StringView;
using facebook::velox::vector_size_t;

constexpr int32_t kMinimumColumnWidth = 3;

// Calls 'callback' with each code point of the UTF-8 'text', its byte offset
// and its length in bytes. An invalid byte counts as one replacement character,
// as Java decodes it.
template <typename Callback>
void forEachCodePoint(std::string_view text, Callback callback) {
  for (size_t i = 0; i < text.size();) {
    int size = 1;
    char32_t codePoint = 0xFFFD;
    if (const auto decoded = utf8proc_codepoint(
            text.data() + i, text.data() + text.size(), size);
        decoded >= 0) {
      codePoint = decoded;
    } else {
      size = 1;
    }
    callback(codePoint, i, size);
    i += size;
  }
}

// Number of UTF-16 code units of 'codePoint', the unit of Java's
// String.length().
int32_t utf16Length(char32_t codePoint) {
  return codePoint > 0xFFFF ? 2 : 1;
}

int32_t utf16Length(std::string_view text) {
  int32_t length = 0;
  forEachCodePoint(text, [&](char32_t codePoint, size_t, size_t) {
    length += utf16Length(codePoint);
  });
  return length;
}

// Whether 'codePoint' is in the full-width ranges of Spark's
// Utils.fullWidthRegex.
bool isFullWidth(char32_t codePoint) {
  static constexpr std::array<std::pair<char32_t, char32_t>, 8> kRanges = {{
      {0x1100, 0x115F},
      {0x2E80, 0xA4CF},
      {0xAC00, 0xD7A3},
      {0xF900, 0xFAFF},
      {0xFE10, 0xFE19},
      {0xFE30, 0xFE6F},
      {0xFF00, 0xFF60},
      {0xFFE0, 0xFFE6},
  }};
  return std::any_of(kRanges.begin(), kRanges.end(), [&](const auto& range) {
    return codePoint >= range.first && codePoint <= range.second;
  });
}

// Spark's Utils.stringHalfWidth: the columns 'text' takes when printed, with a
// full-width character taking two.
int32_t halfWidth(std::string_view text) {
  int32_t width = 0;
  forEachCodePoint(text, [&](char32_t codePoint, size_t, size_t) {
    width += utf16Length(codePoint) + (isFullWidth(codePoint) ? 1 : 0);
  });
  return width;
}

// The longest prefix of 'text' with at most 'units' UTF-16 code units.
std::string_view prefix(std::string_view text, int32_t units) {
  size_t end = 0;
  int32_t length = 0;
  forEachCodePoint(text, [&](char32_t codePoint, size_t start, size_t bytes) {
    length += utf16Length(codePoint);
    if (length <= units) {
      end = start + bytes;
    }
  });
  return text.substr(0, end);
}

// Spark's SchemaUtils.escapeMetaCharacters: control characters that would
// break the table print as escape sequences.
std::string escape(std::string_view text) {
  std::string escaped;
  escaped.reserve(text.size());
  for (char character : text) {
    switch (character) {
      case '\n':
        escaped += "\\n";
        break;
      case '\r':
        escaped += "\\r";
        break;
      case '\t':
        escaped += "\\t";
        break;
      case '\f':
        escaped += "\\f";
        break;
      case '\b':
        escaped += "\\b";
        break;
      case '\v':
        escaped += "\\v";
        break;
      case '\a':
        escaped += "\\a";
        break;
      default:
        escaped += character;
    }
  }
  return escaped;
}

// Binary prints as its bytes in hex, as in "[68 65 6C]".
std::string hex(std::string_view bytes) {
  std::string text = "[";
  for (size_t i = 0; i < bytes.size(); ++i) {
    if (i > 0) {
      text += ' ';
    }
    text += fmt::format("{:02X}", static_cast<unsigned char>(bytes[i]));
  }
  return text + "]";
}

std::string cell(
    const DecodedVector& column,
    vector_size_t row,
    bool binary,
    int32_t truncate) {
  std::string text = "null";
  if (!column.isNullAt(row)) {
    const auto value = column.valueAt<StringView>(row);
    const std::string_view view(value.data(), value.size());
    text = binary ? hex(view) : escape(view);
  }
  if (truncate <= 0 || utf16Length(text) <= truncate) {
    return text;
  }
  if (truncate < 4) {
    return std::string(prefix(text, truncate));
  }
  return std::string(prefix(text, truncate - 3)) + "...";
}

std::string pad(const std::string& text, int32_t width, bool left) {
  const std::string padding(std::max(width - halfWidth(text), 0), ' ');
  return left ? padding + text : text + padding;
}

using Row = std::vector<std::string>;

std::string
formatTable(const Row& header, const std::vector<Row>& data, bool alignRight) {
  std::vector<int32_t> widths(header.size(), kMinimumColumnWidth);
  for (size_t i = 0; i < header.size(); ++i) {
    widths[i] = std::max(widths[i], halfWidth(header[i]));
    for (const auto& row : data) {
      widths[i] = std::max(widths[i], halfWidth(row[i]));
    }
  }

  std::string separator = "+";
  for (auto width : widths) {
    separator += std::string(width, '-') + "+";
  }
  separator += "\n";

  auto line = [&](const Row& row) {
    std::string text = "|";
    for (size_t i = 0; i < row.size(); ++i) {
      text += pad(row[i], widths[i], alignRight) + "|";
    }
    return text + "\n";
  };

  std::string text = separator + line(header) + separator;
  for (const auto& row : data) {
    text += line(row);
  }
  return text + separator;
}

std::string formatVertical(const Row& header, const std::vector<Row>& data) {
  int32_t nameWidth = kMinimumColumnWidth;
  for (const auto& name : header) {
    nameWidth = std::max(nameWidth, halfWidth(name));
  }
  int32_t valueWidth = kMinimumColumnWidth;
  for (const auto& row : data) {
    for (const auto& value : row) {
      valueWidth = std::max(valueWidth, halfWidth(value));
    }
  }

  std::string text;
  for (size_t i = 0; i < data.size(); ++i) {
    auto record = fmt::format("-RECORD {}", i);
    record.resize(
        std::max<size_t>(record.size(), nameWidth + valueWidth + 5), '-');
    text += record + "\n";
    for (size_t j = 0; j < header.size(); ++j) {
      text += " " + pad(header[j], nameWidth, false) + " | " +
          pad(data[i][j], valueWidth, false) + " \n";
    }
  }
  return text;
}

} // namespace

std::string ShowStringFormatter::format(
    const facebook::velox::RowTypePtr& type,
    const std::vector<facebook::velox::RowVectorPtr>& rows,
    int32_t numRows,
    int32_t truncate,
    bool vertical) {
  numRows = std::max(numRows, 0);

  Row header;
  for (const auto& name : type->names()) {
    header.push_back(escape(name));
  }

  // Collects up to numRows + 1 rows; the extra row only says there is more.
  std::vector<Row> data;
  for (const auto& vector : rows) {
    VELOX_CHECK(vector->type()->equivalent(*type));
    const SelectivityVector allRows(vector->size());
    std::vector<DecodedVector> columns;
    for (const auto& child : vector->children()) {
      columns.emplace_back(*child, allRows);
    }
    for (vector_size_t row = 0;
         row < vector->size() && data.size() <= static_cast<size_t>(numRows);
         ++row) {
      Row cells;
      for (size_t i = 0; i < columns.size(); ++i) {
        cells.push_back(
            cell(columns[i], row, type->childAt(i)->isVarbinary(), truncate));
      }
      data.push_back(std::move(cells));
    }
  }
  const bool hasMoreData = data.size() > static_cast<size_t>(numRows);
  data.resize(std::min(data.size(), static_cast<size_t>(numRows)));

  auto text = vertical ? formatVertical(header, data)
                       : formatTable(header, data, truncate > 0);
  if (vertical && data.empty()) {
    text += "(0 rows)\n";
  } else if (hasMoreData) {
    text += fmt::format(
        "only showing top {} {}\n", numRows, numRows == 1 ? "row" : "rows");
  }
  return text;
}

} // namespace axiom::spark_connect
