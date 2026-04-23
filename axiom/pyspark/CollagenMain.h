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

#include <grpcpp/grpcpp.h> // @manual=//grpc_fb/cpp:grpc
#include <memory>
#include <string>

#include "axiom/pyspark/CollagenService.h"

namespace axiom::collagen {

class CollagenMain {
 public:
  CollagenMain(
      std::string runnerId,
      std::string catalog,
      std::string schema,
      int port);

  virtual ~CollagenMain();

  virtual void init();

  void run();

 protected:
  virtual void initMemory();
  virtual void registerRunner();
  virtual void registerSerde();
  virtual void registerFileSystems();
  virtual void registerConnector();
  virtual void registerFunctions();

  void registerTpchConnector();
  void registerTestConnector();
  void registerLocalHiveConnector();

  const std::string runnerId_;
  const std::string catalog_;
  const std::string schema_;
  const int port_;

  std::unique_ptr<CollagenService> service_;
  std::unique_ptr<grpc::Server> server_;
};

} // namespace axiom::collagen
