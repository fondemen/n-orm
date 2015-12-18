Persisting elements are stored into a [column-oriented data model](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/storeapi/Store.html). An example (naive) implementation for the data model is the [Memory](http://code.google.com/p/n-orm/source/browse/storage/src/main/java/com/googlecode/n_orm/memory/Memory.java) class. Here is a comparison between Java elements and data model elements from the point of view of n-orm:
  * A class is some kind of table
  * An object instance of class is some kind of row
  * An object's [identifier](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/PersistingElement.html#getIdentifier()) is the key for the row
  * A Set or Map attribute is some kind of column family ; contained objects (or their identifier in case of persisting objects) are some kind of column values.
  * The set of properties of a class is a special column family.
  * Inheritance make an empty row of a [similar identifier](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/PersistingElement.html#getFullIdentifier()) appear in tables for superclasses.

| **"n-ormalized" Java** | **HBase parlance** |
|:-----------------------|:-------------------|
| Class                  | Table              |
| Instance / Object      | Row                |
|[identifier](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/PersistingElement.html#getIdentifier()) made of all key values | Row id             |
| Map or Set property (made of elements) | Column family (made of columns) |
| Element in Map indexed with a key | Column with value identifying element with a qualifier |
| Element in a Set       | Column with with qualifier identifying element and an empty value |
| Simple property        | The "props" column family |