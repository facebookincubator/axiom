// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <string>

namespace facebook::axiom::velox_udf {

void registerDfMockUdfs(const std::string& prefix = "");

} // namespace facebook::axiom::velox_udf
