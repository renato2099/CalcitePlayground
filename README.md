# CalcitePlayground

There are two simple classes that can be executed: CalciteInMemory and CalciteMysql.

For CalciteMysql please note:

- All database settings are hardcoded please check to [values](src/main/java/org/ethz/systemsgroup/calcite/CalciteMysql.java).
- The database schema used is in [here](https://github.com/renato2099/CalcitePlayground/blob/master/src/main/resources/db_test.sql).

- Please do create such database before executing.

      mysql -hhostname -uuser database < path/to/test.sql
