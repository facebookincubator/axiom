// Copyright 2004-present Facebook. All Rights Reserved.

#pragma once

#include <string>
#include <variant>
#include <vector>

#include <velox/expression/VectorFunction.h>
#include <velox/type/Type.h>
#include <velox/vector/DecodedVector.h>

namespace facebook::axiom::velox_udf {

int64_t readIntegerConstant(const velox::DecodedVector&);
int64_t readIntegerConstant(const velox::exec::VectorFunctionArg&);

bool readBooleanConstant(const velox::DecodedVector&);
bool readBooleanConstant(const velox::exec::VectorFunctionArg&);

template <typename T>
T readRealConstant(const velox::DecodedVector&);

template <>
inline float readRealConstant(const velox::DecodedVector& decoded) {
  VELOX_CHECK(decoded.isConstantMapping());
  return decoded.valueAt<float>(0);
}

template <>
inline double readRealConstant(const velox::DecodedVector& decoded) {
  VELOX_CHECK(decoded.isConstantMapping());
  return decoded.valueAt<double>(0);
}

std::string toVeloxTypeVariableString(velox::TypePtr);

} // namespace facebook::axiom::velox_udf
