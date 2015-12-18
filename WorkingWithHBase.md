The first developed driver for n-orm targets [HBase](http://hbase.apache.org/).
At the time of writing, tested versions for HBase are 0.90.{0,1,2,3,4}, and Cloudera cdh3-u{0,1,3}.
An example all-configured project (with [Eclipse](http://www.eclipse.org/)+[M2Eclipse](http://www.eclipse.org/m2e/)+[AJDT](http://www.eclipse.org/ajdt/)) is the [sample project](http://code.google.com/p/n-orm/source/browse/sample), which can be [downloaded](http://code.google.com/p/n-orm/downloads/detail?name=sample.zip&can=2&q=).



# Maven integration #

See the [getting started](GettingStarted#Using_n-orm_with_maven.md) article, and the [pom](http://code.google.com/p/n-orm/source/browse/sample/pom.xml) for the sample project.

Instead of importing the `store` artifact, you can use the `hbase` one:
```
<dependency>
  <groupId>com.googlecode.n_orm</groupId>
  <artifactId>hbase</artifactId>
  <version>hbase-${hbase.version}-n_orm-${n-orm.version}</version>
  <type>jar</type>
  <scope>compile</scope>
</dependency>
```

# `store.properties` #
To store your objects using HBase, you need to specify a `store.properties` file as the following:
```
class=com.googlecode.n_orm.hbase.HBase
static-accessor=getStore
1=/usr/lib/hadoop,/usr/lib/hbase
```

  * `class` is the name of the [HBase driver](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/HBase.html)
  * `static-accessor=getStore` means that the driver will be retrieved (or instanciated) thanks to the [getStore](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/HBase.html#getStore(java.lang.String)) static method of the driver.
  * `1` is the value sent to [getStore](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/HBase.html#getStore(java.lang.String)) as parameter, i.e. a list of places where to find the HBase (and Hadoop) configuration files, binaries and dependencies

Please, note that an [hbase-site.xml](http://hbase.apache.org/book/config.files.html) must be found in the given folders.

# Where to place `store.properties` ? #

The `store.properties` file will be looked up in the classpath for each [persisting](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Persisting.html) class, first in the same package, then in the package above, etc. For instance, for a classpath set to `srcfolder1:src/folder2:jar1.jar`, the store file for class a.b.C will be searched in the following places:
  1. `srcfolder1/a/b/store.properties`
  1. `src/folder2/a/b/store.properties`
  1. `a/b/store.properties` from jar file `jar1.jar`
  1. `srcfolder1/a/store.properties`
  1. `src/folder2/a/store.properties`
  1. `a/store.properties` from jar file `jar1.jar`
  1. `srcfolder1/store.properties`
  1. `src/folder2/store.properties`
  1. `store.properties` from jar file `jar1.jar`
The first found file is the right file.

# How many HBase stores ? #

Of course, different [persisting](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Persisting.html) classes can have HBase as a store. You can put them all together in the same package, or in different sub-packages of the same package, or in completely different packages. This means that you might need to set more than one `store.properties` files. Anyway, the targeted store will be the same provided you supply the same value for `1` in all those files.

To help you manage stores, another mean is to reference a `store.properties` within a `store.properties` using the `as-for-package` property. As an example, if you want classes of package `a.c` to have the same store as for classes of package `a.b`, just state the following in `a/c/store.properties`:
```
as-for-package=a.b
```
Any other property within this file will be merely ignored.

# Alternative driver #

Actually, the HBase driver comes into two flavors: [com.googlecode.n\_orm.hbase.HBase](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/HBase.html) (as shown above) and [com.googlecode.n\_orm.hbase.Store](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/Store.html).

Compared to [com.googlecode.n\_orm.hbase.HBase](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/HBase.html), [com.googlecode.n\_orm.hbase.Store](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/Store.html) does not add to the CLASSPATH the jar file that can be found in the directories (or sub-directories) given in the `1` parameter. This means that if you use `com.googlecode.n_orm.hbase.Store`, you'll need to explicitly add HBase dependencies to your CLASSPATH at runtime.

Moreover, [com.googlecode.n\_orm.hbase.Store](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/Store.html) adds a [different mean](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/Store.html#getStore(java.lang.String,%20int)) to find an HBase cluster by supplying the name of the host for the zookeeper server managing the HBase cluster as property `1`, and its port as property `2` (usually 2181), as in the following example:
```
class=com.googlecode.n_orm.hbase.Store
static-accessor=getStore
1=localhost
2=2181
```
This second method for accessing an HBase cluster is simpler, and does not requires the HBase jars to be available for the client application using n-orm. However, it must be seen as an alternative solution, since it will depend on the HBase version actually running on the cluster (thus you need to upgrade your client applications as soon as you upgrade your HBase cluster), and since same behavior can be achieved by [configuring hbase-site.xml](http://hbase.apache.org/book/config.files.html#hbase_default_configurations) (see for instance `hbase.client.*` and `hbase.zookeeper.quorum`).

PS: another client library for HBase is [asynchbase](https://github.com/stumbleupon/asynchbase) ; we are still looking for some volunteers to implement a store....

# Testing #

As the `store.properties` file(s) are looked from the CLASSPATH at runtime, a good idea for testing is to have different versions of them, one for testing, and another for production. This is the case for the [sample](http://code.google.com/p/n-orm/source/browse/sample) project, where a version can be found at test-time (i.e. when invoking `mvn test`) on [src/test/resources](http://code.google.com/p/n-orm/source/browse/sample/src/test/resources/com/googlecode/n_orm/sample/businessmodel/store.properties), and another, for production, on [src/main/resources](http://code.google.com/p/n-orm/source/browse/sample/src/main/resources/com/googlecode/n_orm/sample/businessmodel/store.properties) that is jar-packaged by running `mvn package` or `mvn install`.

Note that for testing, you can use the memory driver, which does not invoke HBase at all, with the folllowing `store.properties`:
```
class=com.googlecode.n_orm.memory.Memory
singleton=INSTANCE
```

If you still want to perform your unit testing using HBase, consider using the [com.googlecode.n\_orm.hbase.Store](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/Store.html) driver and adding the HBase dependencies while executing your tests. To help you write your tests, you can use our `hbase-test-deps` maven dependency in your pom.xml, in addition to test jar for the HBase driver:
```
<dependency>
  <groupId>com.googlecode.n_orm</groupId>
  <artifactId>hbase-test-deps</artifactId>
  <version>hbase-${hbase.version}-SNAPSHOT</version>
  <type>pom</type>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>com.googlecode.n_orm</groupId>
  <artifactId>hbase</artifactId>
  <version>hbase-${hbase.version}-n-orm-${n-orm.version}</version>
  <type>test-jar</type>
  <scope>test</scope>
</dependency>
```
The above dependencies make it possible to start an HBase "mini-cluster" as a preamble for your tests (e.g. in a [@BeforeClass](http://junit.sourceforge.net/javadoc/org/junit/BeforeClass.html) method):
```
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.hbase.HBaseLauncher;
import com.googlecode.n_orm.hbase.Store;
...
Properties p;
try {
  //Finding properties for our sample classes
  p = StoreSelector.getInstance().findProperties(MyPersistingClass.class);
  assert Store.class.getName().equals(p.getProperty("class"));
  //Setting them to the test HBase instance
  HBaseLauncher.hbaseHost = p.getProperty("1");
  HBaseLauncher.hbasePort = Integer.parseInt(p.getProperty("2"));
  Store store = Store.getStore(HBaseLauncher.hbaseHost, HBaseLauncher.hbasePort);
  HBaseLauncher.prepareHBase();
  if (HBaseLauncher.hBaseServer != null) {
    //Cheating the actual store for it to point the test data store
    HBaseLauncher.setConf(store);
  }
} catch (IOException e) {
  throw new RuntimeException(e);
}
```

# Driver properties #

Even though you can describe some of the behavior of the client HBase driver using [hbase-site.xml](http://hbase.apache.org/book/config.files.html), n-orm let you set some of them in the `store.properties` file. Any property of the store (as defined by java beans) can be set in the file, as exemplified by the following `store.properties`:
```
class=com.googlecode.n_orm.hbase.HBase
static-accessor=getStore
1=/usr/lib/hadoop,/usr/lib/hbase
compression=gz
scanCaching=20
```
The latter file states that the `compression` should be set to `gz` and `scanCaching` should be set to `20`.

Properties are defined in the [javadoc for the com.googlecode.n\_orm.hbase.Store driver](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/Store.html). The `com.googlecode.n_orm.hbase.HBase` driver takes all `com.googlecode.n_orm.hbase.Store` properties into account.

If you use different `store.properties` files, take great care that they are exactly the same, or use `as-for-package` as explained in section [#How\_many\_HBase\_stores\_?](#How_many_HBase_stores_?.md)

Those properties can be overloaded at class or column-family level using the [HBaseSchema](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/HBaseSchema.html) annotation. For instance, for the following `Foo` class, properties will be saved in an HBase column family with no compression (though set to "gz" by `store.properties`), while the elements in the `elements` column familiy will be saved in an HBase column family with snappy compression:
```
@Persisting
@HBaseSchema(compression="none")
public class Foo {
  @Key public String key;
  public String aProperty, anotherProperty;
  @HBaseSchema(compression="snappy") public SetColumnFamily<String> elements = new SetColumnFamily<String>();
}
```

Some of those properties (such as `compression` or `inMemory`) are taken into account when creating HBase tables and column families. However, in case those tables and column families are already created, by default, they are not altered even though they do not correspond to the desired property (e.g. a column family with no compression while `store.properties` or [@HBaseSchema](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/HBaseSchema.html#compression()) requires "gz". To force alteration, there exist properties `forceXXX` (e.g. forceCompression, both in the [driver](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/Store.html#setInMemory(boolean)) and in the [annotation](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/HBaseSchema.html#forceCompression())) to force n-orm alter the database schema. However, you should be careful with those forcing properties. First, altering a database schema can be a very long task. Note that if n-orm synchronizes itself to avoid different processes (including in different JVMs) to alter the same table at same time, this is not the case for other processes. Second, if two n-orm clients (e.g; in different JVMs) are forced different values for the same property (e.g. in case of incremental upgrade), tables will be altered in an endless loop.

# Performance considerations #

Besides reading some important chapters of the HBase book (e.g.  [client configuration](http://hbase.apache.org/book/important_configurations.html), [schema issues](http://hbase.apache.org/book/perf.schema.html), or [writing](http://hbase.apache.org/book/perf.writing.html), [reading](http://hbase.apache.org/book/perf.reading.html), [deleting](http://hbase.apache.org/book/perf.deleting.html)) here are some few advices regarding using n-orm for HBase:

HBase privileges consistency, as only one server (a region server) will be responsible for a given row (i.e. persisting element instance). Regions, served by a region server, are made of consecutive objects. If you write a lot of objects in the same region, you'll end up in a over-occupied region server. Try to store elements in different regions.

The previous remark has multiple consequences when writing your persisting model.
A first advice is to avoid using at a given time contiguous persisting objects, i.e. objects of the same class with an [id](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/PersistingElement.html#getIdentifier()) starting with the same pattern. One [famous example](http://hbase.apache.org/book/rowkey.design.html) is to avoid to use the current timestamp as first key. This is no longer a problem for keys with an [order above](WritingModels#Keys_to_identify_a_persisting_element.md)  as long as the first key makes objects "far-way" from each other so that they "excite" different region servers. If you write a lot of object of the same table, try to [check you have enough regions](http://hbase.apache.org/book/perf.writing.html) already.

Another-HBase specificity is that it does not like handling too much column families. Prefer creating new objects to adding them to a family, as it is the case in the sample project (see getBooks in [BookStore](http://code.google.com/p/n-orm/source/browse/sample/src/main/java/com/googlecode/n_orm/sample/businessmodel/BookStore.java)).

Consider carefully the meaning of the [different properties](https://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/Store.html) of the HBase driver:
  * [Compression](http://hbase.apache.org/book/compression.html) eases the problem of reading from/writing to the file system as IOs is often a bottleneck
  * [Scan caching](http://hbase.apache.org/book/perf.reading.html) avoids querying the server each time you as for an new object of a search query. If set too high, n-orm might face timeout and need to reconnect, if set too low, HBase cluster will be over queried.
  * [max versions](http://hbase.apache.org/book/schema.versions.html) can be set to 1 since n-orm does not use history
  * etc.type