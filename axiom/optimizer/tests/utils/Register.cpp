// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#include "axiom/optimizer/tests/utils/Register.h"
#include "axiom/optimizer/tests/utils/Bucketize.h"
#include "axiom/optimizer/tests/utils/ConvertFormat.h"
#include "axiom/optimizer/tests/utils/FirstX.h"
#include "axiom/optimizer/tests/utils/SigridHash.h"

namespace facebook::axiom::velox_udf {

void registerDfMockUdfs(const std::string& prefix) {
  registerSigridHash(prefix);
  registerBucketize(prefix);
  registerFirstX();
  registerConvertFormat(prefix);
}

} // namespace facebook::axiom::velox_udf
