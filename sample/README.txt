This is an example project using n-orm for HBase.
See http://n-orm.googlecode.com for more details and updates (look at sources).

To run the project with no HBase instance started on localhost, just run maven (maven 2 required - it takes a while for HBase client to determine that HBase is not running):
mvn test

To run the project on a running HBase, adapt file (if not on localhost)
/src/test/tesources/com/googlecode/n_orm/sample/business/store.poperties
so that it points to the correct HBase configuration. You can then run "mvn test" again.

To test the project using the simple (naive) memory store:
mvn test -f example-memory-pom.xml
To test the project using the jedis-based redis store (redis-server should be started on localhost):
mvn test -f example-memory-pom.xml

For production, you should prefer the variant presented in src/main/resources/com/googlecode/n_orm/sample/business/store.properties.

Files store.properties may appear anywhere in the classpath, anywhere in the package hierarchy (deepest found first).

To create a jar for sample into "target" directory:
mvn package

To create a self-contained jar (without HBase dependencies):
mvn package assembly:single

To run the self-contained jar:
 - start your HBase cluster
 - point to your HBase installation (with HBase configuration and jars) into src/main/resources/com/googlecode/n_orm/sample/business/store.poperties
 - mvn clean package assembly:single (this will temporarily create some test objects into your HBase ; start the cluster only after if you want it not to be polluted by test data)
 - java -ea -jar target/sample-hbase-<HBase version>-n_orm-<n-orm version>-jar-with-dependencies.jar

If you want to set a strong dependency to HBase (e.g. to use com.googlecode.n_orm.hbase.Store instead of com.googlecode.n_orm.hbase.HBase and/or have an assembly that includes HBase and all its dependencies), add the following to pom.xml (<dependencies> section):
<dependency>
	<groupId>org.apache.hbase</groupId>
	<artifactId>hbase</artifactId>
	<version>${hbase.version}</version>
</dependency>
