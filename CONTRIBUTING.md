# Contributing

Verax is an experimental frontned for the Velox ecosystem. Please refer to
the contribution guidelines
[here](https://github.com/facebookincubator/velox/blob/main/CONTRIBUTING.md)


## CMake

### About targets separation

Only targets that really can be used in separate (not in tests, by user) should be separate targets.

So targets like axiom_presto_sql probably makes sense.
But targets like axiom_presto_sql_ast/grammar isn't.
Because the users of these targets anyway will use both.

Example of good separation is axiom_logical_plan and axiom_logical_plan_builder. Some users will use only axiom_logical_plan.

### Notices

* Don't use velox_add_library/etc
* Ideally you should specify PUBLIC/PRIVATE/INTERFACE for linked to target libraries (in target_link_library command). If we need to simplify this, commonly if something only used in .cpp => private, otherwise public. If it's not used, but for some reason you need it => interface. 
