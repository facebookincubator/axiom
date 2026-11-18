# `AT TIME ZONE` type validation

`AT TIME ZONE` is a SQL operator with its own type rule. Axiom represents it as
a special form until it lowers to a Velox function.

## Operand rules

| Operand | Result | Runtime lowering |
|---|---|---|
| `TIMESTAMP` | `TIMESTAMP WITH TIME ZONE` | `at_timezone` |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP WITH TIME ZONE` | `at_timezone` |
| `TIME` | `TIME WITH TIME ZONE` | Cast to `TIME WITH TIME ZONE`, then `at_timezone` |
| `TIME WITH TIME ZONE` | `TIME WITH TIME ZONE` | `at_timezone` |
| `DATE` | Error | None |

The Presto parser carries the operator to type resolution as an internal call
marker. Type resolution checks the value before ordinary function coercion,
casts `TIME` to `TIME WITH TIME ZONE`, and resolves the `at_timezone` overload
by exact signature. The result is an `AT_TIME_ZONE` special form, which stays
one through optimization and becomes a call to `at_timezone` when the Velox
expression is emitted.

Velox owns the timestamp representations and runtime function invariants. See
the [Velox timestamp documentation](https://facebookincubator.github.io/velox/develop/timestamp.html).

## Why the operator is a special form

In Presto SQL, `AT TIME ZONE` has its own type rule: the value must be a time
or timestamp, with or without a time zone. A function call follows the general
coercion rules, under which `DATE` coerces to `TIMESTAMP`. The operator is
therefore a special form, and direct `at_timezone(...)` calls keep the general
rules.

## Test strategy

Parser and resolver tests cover accepted and rejected operand types and
aggregate traversal. Optimizer tests cover lowering, constant folding, and the
null behavior that lets a filter on the operator turn an outer join into an
inner join.

## Not supported yet

- A time zone given as an interval, such as
  `ts AT TIME ZONE INTERVAL '08:00' HOUR TO MINUTE`, fails during type
  resolution. Supporting it needs an `at_timezone` overload in Velox that
  takes an interval; the registered overloads take a `VARCHAR` time zone.
