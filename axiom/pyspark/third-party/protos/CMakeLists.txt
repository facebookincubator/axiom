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
  COLLAGEN_PROTO_SRCS
  base.proto
  catalog.proto
  commands.proto
  common.proto
  expressions.proto
  relations.proto
  types.proto
)

set(COLLAGEN_PROTO_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}")

# Generate C++ sources from .proto files.
set(COLLAGEN_PROTO_CXX_SRCS)
set(COLLAGEN_PROTO_CXX_HDRS)
set(COLLAGEN_GRPC_CXX_SRCS)
set(COLLAGEN_GRPC_CXX_HDRS)

foreach(PROTO_FILE ${COLLAGEN_PROTO_SRCS})
  get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)

  list(APPEND COLLAGEN_PROTO_CXX_SRCS "${COLLAGEN_PROTO_GENERATED_DIR}/${PROTO_NAME}.pb.cc")
  list(APPEND COLLAGEN_PROTO_CXX_HDRS "${COLLAGEN_PROTO_GENERATED_DIR}/${PROTO_NAME}.pb.h")
  list(APPEND COLLAGEN_GRPC_CXX_SRCS "${COLLAGEN_PROTO_GENERATED_DIR}/${PROTO_NAME}.grpc.pb.cc")
  list(APPEND COLLAGEN_GRPC_CXX_HDRS "${COLLAGEN_PROTO_GENERATED_DIR}/${PROTO_NAME}.grpc.pb.h")
endforeach()

find_program(PROTOC_EXECUTABLE protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

add_custom_command(
  OUTPUT
    ${COLLAGEN_PROTO_CXX_SRCS}
    ${COLLAGEN_PROTO_CXX_HDRS}
    ${COLLAGEN_GRPC_CXX_SRCS}
    ${COLLAGEN_GRPC_CXX_HDRS}
  COMMAND
    ${PROTOC_EXECUTABLE} --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
    --cpp_out=${COLLAGEN_PROTO_GENERATED_DIR} --grpc_out=${COLLAGEN_PROTO_GENERATED_DIR}
    --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN} ${COLLAGEN_PROTO_SRCS}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${COLLAGEN_PROTO_SRCS}
  COMMENT "Generating Collagen protobuf/gRPC C++ sources"
)

add_library(collagen_proto ${COLLAGEN_PROTO_CXX_SRCS} ${COLLAGEN_GRPC_CXX_SRCS})

target_include_directories(collagen_proto PUBLIC ${COLLAGEN_PROTO_GENERATED_DIR})

target_link_libraries(collagen_proto protobuf::libprotobuf gRPC::grpc++)
