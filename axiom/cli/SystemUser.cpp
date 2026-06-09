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

#include "axiom/cli/SystemUser.h"

#include <pwd.h>
#include <unistd.h>

#include <cstdlib>

namespace axiom::sql {

std::string SystemUser::resolve() {
  const auto* envUser = std::getenv("USER");
  if (envUser != nullptr && *envUser != '\0') {
    return envUser;
  }

  auto* passwd = getpwuid(getuid());
  if (passwd != nullptr) {
    return passwd->pw_name;
  }

  return "unknown";
}

} // namespace axiom::sql
