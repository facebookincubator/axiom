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

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/synthetic/SyntheticTable.h"
#include "velox/common/base/Exceptions.h"
#include "velox/connectors/Connector.h"

namespace facebook::axiom::connector::synthetic {

/// Connector id under which the shared SyntheticExecutionConnector is
/// registered.
inline constexpr std::string_view kSyntheticConnectorId = "$synthetic";

/// Executes the scan for every synthetic table, on behalf of any catalog.
///
/// A catalog normally maps to its own connector, which runs its scans. Here a
/// single "$synthetic" connector serves all catalogs: a catalog opts in through
/// metadata alone (a findTable() hook, see findSyntheticTable) and needs no
/// scan path or split manager of its own. findSyntheticTable() stamps
/// "$synthetic" as the handle's connectorId() so scan and split dispatch
/// resolve here, and keeps the origin catalog in sourceCatalog() so this
/// connector can rebuild the row provider from that catalog's metadata.
///
/// Usage: call registerSyntheticExecutionConnector() once at startup; a catalog
/// then routes its findTable() through findSyntheticTable(), and scans of the
/// returned table dispatch here.
class SyntheticExecutionConnector : public velox::connector::Connector {
 public:
  explicit SyntheticExecutionConnector(const std::string& id) : Connector(id) {}

  /// Rebuilds the row provider over the source catalog's metadata and scans it.
  /// See kSyntheticConnectorId.
  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::connector::ConnectorQueryCtx* connectorQueryCtx) override;

  /// Synthetic tables are read-only.
  std::unique_ptr<velox::connector::DataSink> createDataSink(
      velox::RowTypePtr /*inputType*/,
      velox::connector::ConnectorInsertTableHandlePtr /*insertTableHandle*/,
      velox::connector::ConnectorQueryCtx* /*connectorQueryCtx*/,
      velox::connector::CommitStrategy /*commitStrategy*/) override {
    VELOX_UNSUPPORTED("Synthetic execution connector does not support writes");
  }
};

/// ConnectorMetadata for SyntheticExecutionConnector. A synthetic table is
/// resolved on its origin catalog; only its execution routes to "$synthetic".
/// This metadata therefore owns only a split manager, so that split routing
/// resolves for a synthetic table handle; findTable() and the listing methods
/// are never reached.
class SyntheticExecutionMetadata : public ConnectorMetadata {
 public:
  TablePtr findTable(const SchemaTableName& /*tableName*/) override {
    return nullptr;
  }

  ConnectorSplitManager* splitManager() override {
    return &splitManager_;
  }

  std::vector<std::string> listSchemaNames(
      const ConnectorSessionPtr& /*session*/) override {
    return {};
  }

  bool schemaExists(
      const ConnectorSessionPtr& /*session*/,
      const std::string& /*schemaName*/) override {
    return false;
  }

  bool listTableNamesSupported() const override {
    return false;
  }

  std::vector<std::string> listTableNames(
      const ConnectorSessionPtr& /*session*/,
      const std::string& /*schemaName*/) override {
    return {};
  }

 private:
  SyntheticSplitManager splitManager_;
};

/// Registers the shared execution connector, its metadata, and the synthetic
/// SerDe under kSyntheticConnectorId. A catalog that serves synthetic tables
/// must ensure this has run once. Idempotent. Not thread-safe: call from a
/// single thread (server startup or test setup), never concurrently with itself
/// or unregisterSyntheticExecutionConnector().
void registerSyntheticExecutionConnector();

/// Reverses registerSyntheticExecutionConnector. Idempotent, and subject to the
/// same single-thread requirement.
void unregisterSyntheticExecutionConnector();

/// Returns a SyntheticTable serving 'name' if 'metadata' recognizes it as a
/// synthetic relation (via findSyntheticProvider), or nullptr otherwise. A
/// catalog calls this from its findTable(). 'catalog' is recorded on the
/// returned table's handles as sourceCatalog().
///
/// 'metadata' is read once and not retained. SyntheticExecutionConnector must
/// be registered before this call and stay registered for the returned table's
/// lifetime, since the table holds a raw pointer to it.
TablePtr findSyntheticTable(
    const SchemaTableName& name,
    ConnectorMetadata* metadata,
    std::string_view catalog);

} // namespace facebook::axiom::connector::synthetic
