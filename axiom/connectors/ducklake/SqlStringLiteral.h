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

#pragma once

#include <string>
#include <string_view>

namespace facebook::axiom::connector::ducklake {

/// Formats a value as a single-quoted SQL string literal.
///
/// The returned string includes the surrounding single quotes and escapes any
/// embedded single quotes by doubling them. Use this only for literal values;
/// SQL identifiers require a separate quoting or validation path.
///
/// @param value Literal value to format for SQL text.
/// @returns Quoted SQL string literal.
std::string quoteSqlStringLiteral(std::string_view value);

} // namespace facebook::axiom::connector::ducklake
