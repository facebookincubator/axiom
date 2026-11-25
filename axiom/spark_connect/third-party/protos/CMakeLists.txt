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

set(
  SPARK_CONNECT_PROTO_SRCS
  base.proto
  catalog.proto
  commands.proto
  common.proto
  expressions.proto
  relations.proto
  types.proto
)

# Generate C++ sources from .proto files.
set(SPARK_CONNECT_PROTO_CXX_SRCS)
set(SPARK_CONNECT_PROTO_CXX_HDRS)
set(SPARK_CONNECT_GRPC_CXX_SRCS)
set(SPARK_CONNECT_GRPC_CXX_HDRS)

set(SPARK_CONNECT_PROTO_PATHS)

foreach(PROTO_FILE ${SPARK_CONNECT_PROTO_SRCS})
  file(RELATIVE_PATH PROTO_PATH ${PROJECT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_FILE})
  get_filename_component(PROTO_DIR ${PROTO_PATH} DIRECTORY)
  get_filename_component(PROTO_NAME ${PROTO_PATH} NAME_WE)
  list(APPEND SPARK_CONNECT_PROTO_PATHS ${PROTO_PATH})

  list(APPEND SPARK_CONNECT_PROTO_CXX_SRCS "${PROJECT_BINARY_DIR}/${PROTO_DIR}/${PROTO_NAME}.pb.cc")
  list(APPEND SPARK_CONNECT_PROTO_CXX_HDRS "${PROJECT_BINARY_DIR}/${PROTO_DIR}/${PROTO_NAME}.pb.h")
  list(
    APPEND
    SPARK_CONNECT_GRPC_CXX_SRCS
    "${PROJECT_BINARY_DIR}/${PROTO_DIR}/${PROTO_NAME}.grpc.pb.cc"
  )
  list(
    APPEND
    SPARK_CONNECT_GRPC_CXX_HDRS
    "${PROJECT_BINARY_DIR}/${PROTO_DIR}/${PROTO_NAME}.grpc.pb.h"
  )
endforeach()

find_program(PROTOC_EXECUTABLE protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

add_custom_command(
  OUTPUT
    ${SPARK_CONNECT_PROTO_CXX_SRCS}
    ${SPARK_CONNECT_PROTO_CXX_HDRS}
    ${SPARK_CONNECT_GRPC_CXX_SRCS}
    ${SPARK_CONNECT_GRPC_CXX_HDRS}
  COMMAND
    ${PROTOC_EXECUTABLE} --proto_path=${PROJECT_SOURCE_DIR} --cpp_out=${PROJECT_BINARY_DIR}
    --grpc_out=${PROJECT_BINARY_DIR} --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
    ${SPARK_CONNECT_PROTO_PATHS}
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  DEPENDS ${SPARK_CONNECT_PROTO_SRCS}
  COMMENT "Generating Spark Connect protobuf/gRPC C++ sources"
)

add_library(
  axiom_spark_connect_proto
  ${SPARK_CONNECT_PROTO_CXX_SRCS}
  ${SPARK_CONNECT_GRPC_CXX_SRCS}
)

target_include_directories(axiom_spark_connect_proto PUBLIC ${PROJECT_BINARY_DIR})

target_link_libraries(axiom_spark_connect_proto protobuf::libprotobuf gRPC::grpc++)
