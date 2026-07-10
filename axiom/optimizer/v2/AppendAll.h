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

namespace facebook::axiom::optimizer::v2 {

/// Appends every element of `src` to `dst`. Used to concatenate
/// `ColumnVector` / `ExprVector` when building output column lists or
/// expression lists for new nodes.
template <typename Dst, typename Src>
void appendAll(Dst& dst, const Src& src) {
  dst.insert(dst.end(), src.begin(), src.end());
}

/// Returns a new vector containing the first `count` elements of `src`.
/// Caller must ensure `count <= src.size()`.
template <typename V>
V prefix(const V& src, size_t count) {
  return V(src.begin(), src.begin() + count);
}

} // namespace facebook::axiom::optimizer::v2
