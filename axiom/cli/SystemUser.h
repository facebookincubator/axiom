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

namespace axiom::sql {

/// OS-level user identity lookup. Used by CLI entry points to seed the
/// session user when no application-level identity is available.
class SystemUser {
 public:
  /// Returns the current OS user. Tries `$USER`, then the passwd entry for
  /// the effective UID, then `"unknown"`. Always returns a non-empty string.
  static std::string resolve();
};

} // namespace axiom::sql
