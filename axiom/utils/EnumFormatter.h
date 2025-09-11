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

#include <fmt/format.h>
#include <string_view>
#include "velox/common/Enums.h"

#define AXIOM_ENUM_FORMATTER(enum)                                   \
  template <>                                                        \
  struct fmt::formatter<::enum> : fmt::formatter<std::string_view> { \
    template <typename FormatContext>                                \
    auto format(::enum k, FormatContext& ctx) const {                \
      return fmt::formatter<std::string_view>::format(               \
          ::enum##Name::toName(k), ctx);                             \
    }                                                                \
  }

#define AXIOM_EMBEDDED_ENUM_FORMATTER(class, enum)                           \
  template <>                                                                \
  struct fmt::formatter<::class ::enum> : fmt::formatter<std::string_view> { \
    template <typename FormatContext>                                        \
    auto format(::class ::enum k, FormatContext& ctx) const {                \
      return fmt::formatter<std::string_view>::format(                       \
          ::class ::toName(k), ctx);                                         \
    }                                                                        \
  }
