// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <string>

namespace facebook::axiom::velox_udf {

void registerBucketize(const std::string& prefix = "");

} // namespace facebook::axiom::velox_udf
