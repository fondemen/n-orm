This is an example project using n-orm for HBase.
See http://n-orm.googlecode.com for more details
and updates (look at sources).

To run the project with no HBase instance started,
just run maven:
mvn test

To run the project on a running HBase, adapt file
/src/test/tesources/com/googlecode/n_orm/sample/business/store.poperties
so that it ponts to the correct HBase configuration.
You can then run mvn test again.

You can perfer one of the variant presented
(e.g. example-hbase-store.properties).
To use it, just rename it store.properties.

Files store.properties may appear anywhere in the classpath,
anywhere in the package hierarchy.
