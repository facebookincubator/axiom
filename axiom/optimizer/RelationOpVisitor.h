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

#include "axiom/optimizer/RelationOp.h"

namespace facebook::axiom::optimizer {

class RelationOpVisitorContext {
 public:
  virtual ~RelationOpVisitorContext() = default;
};

class RelationOpVisitor {
 public:
  virtual ~RelationOpVisitor() = default;

  virtual void visit(const TableScan& op, RelationOpVisitorContext& context)
      const = 0;

  virtual void visit(const Repartition& op, RelationOpVisitorContext& context)
      const = 0;

  virtual void visit(const Filter& op, RelationOpVisitorContext& context)
      const = 0;

  virtual void visit(const Project& op, RelationOpVisitorContext& context)
      const = 0;

  virtual void visit(const Join& op, RelationOpVisitorContext& context)
      const = 0;

  virtual void visit(const HashBuild& op, RelationOpVisitorContext& context)
      const = 0;

  virtual void visit(const Aggregation& op, RelationOpVisitorContext& context)
      const = 0;

  virtual void visit(const OrderBy& op, RelationOpVisitorContext& context)
      const = 0;

  virtual void visit(const UnionAll& op, RelationOpVisitorContext& context)
      const = 0;

  virtual void visit(const Limit& op, RelationOpVisitorContext& context)
      const = 0;

  virtual void visit(const Values& op, RelationOpVisitorContext& context)
      const = 0;

  virtual void visit(const Unnest& op, RelationOpVisitorContext& context)
      const = 0;

  virtual void visit(const TableWrite& op, RelationOpVisitorContext& context)
      const = 0;
};

} // namespace facebook::axiom::optimizer
