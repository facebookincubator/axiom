#include <folly/String.h>
#include <sstream>

#include "axiom/optimizer/tests/utils/Utils.h"

namespace facebook::axiom::velox_udf {

using namespace facebook::velox;

int64_t readIntegerConstant(const DecodedVector& decoded) {
  VELOX_CHECK(decoded.isConstantMapping());
  auto kind = decoded.base()->typeKind();
  switch (kind) {
    case TypeKind::INTEGER:
      return decoded.valueAt<int32_t>(0);
    case TypeKind::BIGINT:
      return decoded.valueAt<int64_t>(0);
    default:
      VELOX_FAIL("Unexpected kind: {}", kind);
  }
}

int64_t readIntegerConstant(const velox::exec::VectorFunctionArg& arg) {
  VELOX_CHECK(arg.constantValue);
  auto kind = arg.type->kind();
  switch (kind) {
    case TypeKind::INTEGER:
      return arg.constantValue->as<ConstantVector<int32_t>>()->value();
    case TypeKind::BIGINT:
      return arg.constantValue->as<ConstantVector<int64_t>>()->value();
    default:
      VELOX_FAIL("Unexpected kind: {}", kind);
  }
}

bool readBooleanConstant(const DecodedVector& decoded) {
  VELOX_CHECK(decoded.isConstantMapping());
  return decoded.valueAt<bool>(0);
}

bool readBooleanConstant(const velox::exec::VectorFunctionArg& arg) {
  VELOX_CHECK(arg.constantValue);
  return arg.constantValue->as<ConstantVector<bool>>()->value();
}

std::string toVeloxTypeVariableString(TypePtr type) {
  auto kind = type->kind();
  std::stringstream ss;
  switch (kind) {
    case TypeKind::ROW: {
      ss << TypeTraits<TypeKind::ROW>::name << "(";

      const auto& rowType = type->asRow();

      for (auto i = 0; i < rowType.size(); ++i) {
        if (i > 0) {
          ss << ", ";
        }

        const auto& name = rowType.nameOf(i);
        if (!name.empty()) {
          ss << std::quoted(name, '"', '"') << " ";
        }

        ss << toVeloxTypeVariableString(rowType.childAt(i));
      }
      ss << ")";
      break;
    }

    case TypeKind::ARRAY: {
      ss << TypeTraits<TypeKind::ARRAY>::name << "("
         << toVeloxTypeVariableString(type->asArray().elementType()) << ")";
      break;
    }

    case TypeKind::MAP: {
      auto mapType = type->asMap();
      ss << TypeTraits<TypeKind::MAP>::name << "("
         << toVeloxTypeVariableString(mapType.keyType()) << ", "
         << toVeloxTypeVariableString(mapType.valueType()) << ")";
      break;
    }

    case TypeKind::FUNCTION: {
      ss << TypeTraits<TypeKind::FUNCTION>::name << "(";
      std::vector<std::string> inputArgTypes;
      for (auto& child : type->asFunction().children()) {
        inputArgTypes.push_back(toVeloxTypeVariableString(child));
      }
      ss << folly::join(", ", inputArgTypes) << ")";
      break;
    }

    case TypeKind::INVALID:
      VELOX_NYI("INVALID Type kind unsupported");
      break;

    case TypeKind::OPAQUE:
      ss << type->name();
      break;

    case TypeKind::UNKNOWN:
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
    case TypeKind::HUGEINT: {
      ss << kind;
      break;
    }
    default:
      VELOX_UNREACHABLE("Type unsupported: {}", kind);
  }
  return ss.str();
}

} // namespace facebook::axiom::velox_udf
