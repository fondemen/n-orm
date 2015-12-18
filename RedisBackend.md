# Introduction #

[Redis](http://redis.io) is « an open source, advanced key-value store ».
n-orm can use Redis for storing persisting objects.

# Details #

The Redis backend uses [Jedis](https://github.com/xetorthio/jedis), one of the reference Java client for Redis.

It currently supports Redis in a single-master server mode.

## [SimpleStore](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/index.html?com/googlecode/n_orm/storeapi/SimpleStore.html) interface ##
The Redis backend implements the [SimpleStore interface](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/index.html?com/googlecode/n_orm/storeapi/SimpleStore.html).

This means that it will be wrapped by [SimpleStoreWrapper](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/index.html?com/googlecode/n_orm/storeapi/SimpleStoreWrapper.html) to qserve as a data store.

## Data model in Redis ##

Unlike HBase, Redis is not column-oriented, though it can store only different forms of data. A key (a string) refers to a value, which can be :
  * A String
  * [hashes](http://redis.io/topics/data-types#hashes)
  * [lists](http://redis.io/topics/data-types#lists),
  * [sets](http://redis.io/topics/data-types#sets),
  * and [sorted sets](http://redis.io/topics/data-types#sorted-sets).

Because of these limitations, a n-orm persisting object is stored in Redis across multiple keys and values.

### Structure of a row ###
In n-orm, each object is defined by :
  * its _table_ : the class of the object
  * its _id_ : a unique identifier in n-orm
  * its _families_ : a second level data of _table_
  * its _keys_ : the names of the columns
  * its _data_ : the data store in each column, which can be an _increment_ or arbitrary data (stored in base64)

### Structure of data in Redis ###
| **key**                                     | **Redis Type** | **Data** |
|:--------------------------------------------|:---------------|:---------|
| _table_                                     | Sorted Set     | The list of the _id\_s in this table_|
| _table_:families                            | Set            | The list of families in this table |
| _table_:_id_:families:_family_:keys         | Sorted Set     | The list of the keys on this row |
| _table_:_id_:families:_family_:vals         | Hash           | The values associated with the keys in this family |
| _table_:_id_:families:_family_:increments   | Hash           | The increments associated with the keys in this family |

The data is stored in Base64 for the values, and in plain number for the increments.
The Base64 has been selected for preventing errors in saving bytes array from java (the conversion to/from Base64 is made in the RedisStore).
However, Redis supports natively an increment command, so the data is stored in plain number for this type of data.

## Search with [Constraints](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/index.html?com/googlecode/n_orm/storeapi/Constraint.html) ##
n-orm use the [Constraint](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/index.html?com/googlecode/n_orm/storeapi/Constraint.html) class for searching elements or keys.
A Constraint is build with a _startKey_ and a _stopKey_ , which can be null, and can impose a limit of number of results.

Redis does not support search in a Sorted Set, but can return position for an element of the SortedSet.
For implementing the constrained search, RedisStore proceeds as follows: for the start key
1. If the key is null, we return as first element of the search the first key in the Sorted Set
2. Else, if the key exists,
2.a. RedisStore get its position
2.b. Else, RedisStore inserts the key, get its position, and remove it within a transaction.

Result for the searc is a collection starting from the found position, ending only when a key above the end key is found from Redis

## Example for a properties file ##
n-orm can be configured with store.properties files.
This is an example for such a file with RedisStore :
```
# store.properties
class=com.googlecode.n_orm.redis.RedisStore
static-accessor=getStore
1=localhost #the Redis server host name
```

A second parameter can be added to state the server port.

## Code coverage and tests ##
The code is covered by JUnit tests, with more than 94% coverage with RedisGenericTest.
All the tests pass successfully.

## AspectJ and Jedis ##
Jedis is thread safe, but it has to use the JedisPool mecanism.
However, Jedis asks to return the Jedis instance got from the JedisPool.
In order to not pollute the backend with in its get from pool / return to pool code, an Aspect catches every call to Jedis, get a new Jedis instance from the pool and return it after the execution of the Redis command.

Moreover, if a command fails, the Aspect returns the Jedis instance to the Jedis garbage collector and relaunches the command.