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

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "axiom/pyspark/CollagenMain.h"

DEFINE_int32(port, 12345, "TCP port to listen to.");
DEFINE_string(catalog, "tpch", "Catalog to use: test, tpch, hive");
DEFINE_string(schema, "", "Schema to use");
DEFINE_string(runner_id, "local", "Runner id to use, supported: local.");

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  FLAGS_velox_exception_user_stacktrace_enabled = true;

  axiom::collagen::CollagenMain collagenMain(
      FLAGS_runner_id, FLAGS_catalog, FLAGS_schema, FLAGS_port);
  collagenMain.init();
  collagenMain.run();

  return 0;
}
