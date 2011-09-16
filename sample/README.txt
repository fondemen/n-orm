This is an example project using n-orm for HBase.
See http://n-orm.googlecode.com for more details
and updates (look at sources).

To run the project with no HBase instance started,
just run maven:
mvn test

To run the project on a running HBase, adapt file
/src/test/tesources/com/googlecode/n_orm/sample/business/store.poperties
so that it points to the correct HBase configuration.
You can then run mvn test again.

To test the project using the simple (naive) memory store:
mvn test -f example-memory-pom.xml

For production, you can prefer the variant presented
(e.g. src/main/resources/com/googlecode/n_orm/sample/business/store.poperties).

Files store.properties may appear anywhere in the classpath,
anywhere in the package hierarchy.
