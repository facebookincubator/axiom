#include "axiom/common/WriteKind.h"

namespace facebook::axiom {
namespace {

const auto& writeKindNames() {
  static const folly::F14FastMap<WriteKind, std::string_view> kNames = {
      {WriteKind::kCreate, "CREATE"},
      {WriteKind::kInsert, "INSERT"},
      {WriteKind::kUpdate, "UPDATE"},
      {WriteKind::kDelete, "DELETE"},
  };
  return kNames;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(WriteKind, writeKindNames);

} // namespace facebook::axiom
