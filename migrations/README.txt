This is a stripped version of flywaydb (http://flywaydb.org/documentation/commandline/).
Provides support for SQL based migrations.

We are using it as a migration tool for Amazon redshift.
configuration file is in `conf/flyway.conf` which contains redshift connection settings.

DEPENDENCIES:
 * JRE - make sure your JAVA_HOME variable is set.

USAGES:
 * Put sql based migrations in `sql/` dir. the sql migrations must follow a naming pattern
   eg: V1_001__some_description_of_migration.sql (http://flywaydb.org/documentation/migration/sql.html)

RUN:
 > ./flyway info
 > ./flyway migrate

REF: http://flywaydb.org/documentation/commandline/


