#include "axiom/optimizer/BuiltinNames.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::velox::optimizer {

BuiltinNames::BuiltinNames()
    : eq{toName("eq")},
      neq{toName("neq")},
      lt{toName("lt")},
      lte{toName("lte")},
      gt{toName("gt")},
      gte{toName("gte")},
      plus{toName("plus")},
      multiply{toName("multiply")},
      cardinality{toName("cardinality")},
      subscript{toName("subscript")},
      elementAt{toName("element_at")},
      _and{toName(SpecialFormCallNames::kAnd)},
      _or{toName(SpecialFormCallNames::kOr)},
      _cast{toName(SpecialFormCallNames::kCast)},
      _tryCast{toName(SpecialFormCallNames::kTryCast)},
      _try{toName(SpecialFormCallNames::kTry)},
      _coalesce{toName(SpecialFormCallNames::kCoalesce)},
      _if{toName(SpecialFormCallNames::kIf)},
      _switch{toName(SpecialFormCallNames::kSwitch)},
      _in{toName(SpecialFormCallNames::kIn)},
      canonicalizable{
          eq,
          neq,
          lt,
          lte,
          gt,
          gte,
          plus,
          multiply,
          _and,
          _or,
      } {}

Name BuiltinNames::reverse(Name name) const {
  if (name == lt) {
    return gt;
  }
  if (name == lte) {
    return gte;
  }
  if (name == gt) {
    return lt;
  }
  if (name == gte) {
    return lte;
  }
  return name;
}

Name BuiltinNames::crc32() const {
  return toName("crc32");
}

Name BuiltinNames::bitwiseAnd() const {
  return toName("bitwise_and");
}

Name BuiltinNames::bitwiseRightShift() const {
  return toName("bitwise_right_shift");
}

Name BuiltinNames::bitwiseXor() const {
  return toName("bitwise_xor");
}

Name BuiltinNames::hash() const {
  return toName("hash");
}

Name BuiltinNames::mod() const {
  return toName("mod");
}

} // namespace facebook::velox::optimizer
