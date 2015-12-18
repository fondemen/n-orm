# What is n-orm #

n-orm offers a simple persistence layer for Java.
Targeted stores are "no-sql" as the framework assumes a column-oriented like storage data model.

The framework makes use of annotations and a very simple query language, simple enough to be very efficient, and too simple to let a model not to be efficient... ;-)

Current stores are HBase and Redis, though new drivers should be simple to implement.
Expectations on a data store are that it can handle maps automatically sorted according to their keys, and that it supports atomic increments.

n-orm is neither an object-database mapping, nor it addresses specifically relational databases. Its name was chosen as a reference to the No-SQL movement.

To keep n-orm simple, it is not possible to use any Java data model, nor any legacy database. n-orm is more a Java view on a column-oriented database, which means that the data model (i.e. Java classes) has to be written according to certain (simple) constraints.

Different data stores are possible, especially column-oriented data stores such as [HBase](http://hbase.apache.org/), [Cassandra](http://cassandra.apache.org/) or [Google BigTable](http://labs.google.com/papers/bigtable.html), though only HBase, [MongoDB](http://www.mongodb.org) and [Redis](http://redis.io/) are supported so far (help would be appreciated ;-)).

<a href='Hidden comment: 
= Why yet another persistence layer tool ? =

There are many different persistence frameworks around.

Some are quite generic and able to do almost anything, like [http://www.oracle.com/technetwork/java/index-jsp-135919.html JDO]-based persistence frameworks. Those generic tools are oriented towards relational databases, and hide much of the generated data structure. A problem is that column-oriented data stores are very efficient, but you really need to think data structure properly (e.g. structure for keys for efficient search, column grouping). Moreover, it is often the case that those generic tools have a slow learning curve.

Some others, like [https://github.com/ghelmling/meetup.beeno meetup beeno] or [http://code.google.com/p/objectify-appengine/ Objectify], are really simple to use and their behavior is clear. However, they are dedicated to one single kind of data store which might be a problem when you are not sure of the platform you want, when it"s time to migrate platform, or if you want to benefit from advantages of different platforms at same time with an unified mean to describe your data models.
'></a>

n-orm tries to offer a persistence layer with the following objectives:
  * easy to use and learn,
  * possibility to think your data model (i.e. Java classes) as you think your column-oriented data structure,
  * designed for highly concurrent and distributed applications that target same data stores
  * possibility to target different data store with exactly the same data model,
  * prevent bad data structures hard to search (you won't be able to write the query), or dangerous operations like loading million objects into memory.
  * easiness to develop a new data store

# Quick example #

Here is an example of a persisting data class:
```
@Persisting
public class Product {
	@Key(order=1) public String trademark;
	@Key(order=2) public String title;
	public int number;
}
```

Thanks to [aspect-oriented programming](http://en.wikipedia.org/wiki/Aspect-oriented_programming), the [Persisting annotation](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Persisting.html) automatically makes the Product class implement [all necessary behavior](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/PersistingElement.html) for storing and retrieving an element in the data store:
```
Product p = new Product();
p.trademark = "ACME"; //Could be accessed through setters or getters
p.title = "Road Runner trap";
p.number = 198;
p.store();
```
```
Product b = new Product();
p.trademark = "ACME"; //Keys have to be known
p.title = "Road Runner trap";
assert p.number == 0;
p.activate();
assert p.number == 198;
```

An [embedded query language](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/query/ConstraintBuilder.html) may help in case you need to look for elements whose keys are in a given range:
```
Set<Product> prs = StorageManagement.findElements()
	.ofClass(BookStore.class)
	.withKey("trademark").setTo("ACME")
	.withKey("title").between("R").and("S")
	.withAtMost(1000).elements().go();
```

More information is available [to create a data model](WritingModels.md).

See also the [GettingStarted](GettingStarted.md) section to start working.
You may also want to have a look at the [API](API.md).

A [sample project](http://code.google.com/p/n-orm/downloads/detail?name=sample-project.zip) is available so that you can start with something that is immediately working.

# Main features #

  * Transforming a persisting element and its content so that it can enter/be retreived into/from a [Store](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/storeapi/Store.html) ; possible attributes are given [here](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Persisting.html).
  * Inferring identifiers from an ordered set of properties marked as keys.
  * Persisting Set and Maps properties.
  * Inheritance in persisting classes and properties types.
  * Storing only that information that have changed since last activation or store action.
  * No need to generate a schema.
  * [Embedded language](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/query/ConstraintBuilder.html) for searching elements depending on their keys (though not on their properties).
  * [Incrementing](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Incrementing.html) properties and maps.
  * Possibility to use different stores for different parts of your data model.
  * Support for remote procedures (e.g. map/reduce in HBase)
  * [Overloadable store choice](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Persisting.html) (e.g. one store for testing and another for production)
  * [Write-back caching](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/cache/write/WriteRetentionStore.html)
  * [Federated tables](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Persisting.html#federated())
  * [Store activity listenening](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/PersistingElement.html#addPersistingElementListener(com.googlecode.n_orm.PersistingElementListener))

However, n-orm still needs you to properly understand the [DataModel](DataModel.md), so that you can properly think you data model. You may want to look for HBase or Cassandra tutorials around to  get familiar with this new way of thinking data models.

# Contact #

Join us at http://groups.google.com/group/n-orm !