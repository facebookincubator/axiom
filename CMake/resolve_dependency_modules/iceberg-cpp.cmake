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

# Iceberg's bundled Avro reader is needed to plan manifest files. Build and
# install Iceberg separately with scripts/setup-iceberg-cpp.sh, which first
# installs a Velox-version Arrow/Parquet package and then configures iceberg-cpp
# to use those package targets instead of its default vendored Arrow.
find_package(iceberg CONFIG QUIET COMPONENTS REST)

if(TARGET iceberg::iceberg_rest_static)
  set(AXIOM_ICEBERG_REST_TARGET iceberg::iceberg_rest_static)
  set(AXIOM_ICEBERG_DATA_TARGET iceberg::iceberg_data_static)
  set(AXIOM_ICEBERG_BUNDLE_TARGET iceberg::iceberg_bundle_static)
elseif(TARGET iceberg::iceberg_rest_shared)
  set(AXIOM_ICEBERG_REST_TARGET iceberg::iceberg_rest_shared)
  set(AXIOM_ICEBERG_DATA_TARGET iceberg::iceberg_data_shared)
  set(AXIOM_ICEBERG_BUNDLE_TARGET iceberg::iceberg_bundle_shared)
else()
  message(
    FATAL_ERROR
    "Axiom's Iceberg connector requires an installed iceberg-cpp package. "
    "Run scripts/setup-iceberg-cpp.sh, or add its install prefix to "
    "CMAKE_PREFIX_PATH."
  )
endif()

if(TARGET iceberg::arrow_static OR TARGET iceberg::parquet_static)
  message(
    FATAL_ERROR
    "Installed iceberg-cpp package still exports vendored Arrow or Parquet. "
    "Reinstall it using scripts/setup-iceberg-cpp.sh so iceberg-cpp uses the "
    "same Arrow package as Velox."
  )
endif()

foreach(
  _iceberg_target
  IN
  ITEMS ${AXIOM_ICEBERG_REST_TARGET} ${AXIOM_ICEBERG_DATA_TARGET} ${AXIOM_ICEBERG_BUNDLE_TARGET}
)
  if(NOT TARGET ${_iceberg_target})
    message(
      FATAL_ERROR
      "Installed iceberg-cpp package is missing ${_iceberg_target}. "
      "Axiom requires the REST, data, and bundled Avro libraries to plan "
      "Iceberg manifests. Reinstall it using scripts/setup-iceberg-cpp.sh."
    )
  endif()
endforeach()

get_target_property(
  _iceberg_include_dirs
  ${AXIOM_ICEBERG_REST_TARGET}
  INTERFACE_INCLUDE_DIRECTORIES
)
set(_iceberg_headers_found FALSE)
foreach(_iceberg_include_dir IN LISTS _iceberg_include_dirs)
  if(
    EXISTS "${_iceberg_include_dir}/iceberg/catalog/rest/rest_catalog.h"
    AND EXISTS "${_iceberg_include_dir}/iceberg/avro/avro_register.h"
  )
    set(_iceberg_headers_found TRUE)
    break()
  endif()
endforeach()

if(NOT _iceberg_headers_found)
  message(
    FATAL_ERROR
    "Installed iceberg-cpp package is missing REST or Avro headers. "
    "Reinstall it using scripts/setup-iceberg-cpp.sh."
  )
endif()

message(STATUS "Using iceberg-cpp target ${AXIOM_ICEBERG_REST_TARGET}")
unset(_iceberg_headers_found)
unset(_iceberg_include_dir)
unset(_iceberg_include_dirs)
unset(_iceberg_target)
