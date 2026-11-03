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

#include "axiom/optimizer/PhysicalProperties.h"

namespace facebook::axiom::optimizer {

/// A partitioning on a subset of join keys. Positions identify the
/// corresponding key pairs across the two join inputs.
struct JoinPartitioning {
  /// Positions of the partition keys in the join keys, in partition-key order.
  std::vector<size_t> keyPositions;

  /// Connector partition function, or null for standard hash.
  const connector::PartitionType* partitionType;

  /// Describes 'partitioning' in terms of 'joinKeys', or nullopt when it cannot
  /// be reused to co-locate the join.
  static std::optional<JoinPartitioning> from(
      const Partitioning& partitioning,
      const ExprVector& joinKeys);

  /// True when this and 'other' send matching join rows to the same task.
  bool coPartitionsWith(const JoinPartitioning& other) const;

  /// Selects the keys on the side being moved that correspond to this
  /// partitioning.
  ExprVector correspondingKeys(const ExprVector& joinKeys) const;
};

} // namespace facebook::axiom::optimizer
