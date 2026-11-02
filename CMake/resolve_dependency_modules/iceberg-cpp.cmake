# Copyright (c) Meta Platforms, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include_guard(GLOBAL)

# Iceberg's bundled Avro reader is needed to plan manifest files. Iceberg-cpp
# uses a newer Arrow/Parquet release than Velox, so the setup script installs
# both under their own prefix rather than replacing Velox's Arrow 18 package.
set(
  AXIOM_ICEBERG_CPP_PREFIX
  "${PROJECT_SOURCE_DIR}/deps-install/iceberg-cpp"
  CACHE PATH
  "Prefix containing the isolated iceberg-cpp and Arrow packages."
)

# The iceberg-cpp package brings imported targets for its own Arrow, with the
# same names Velox's Arrow uses. Resolving it in a directory of its own keeps
# those targets out of every other part of the build; only axiom_iceberg_cpp
# leaves that directory.
add_subdirectory("${CMAKE_CURRENT_LIST_DIR}/iceberg-cpp" "${CMAKE_BINARY_DIR}/iceberg-cpp")
