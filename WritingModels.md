# Persisting elements #
Data classes have a certain number of [constraints](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Persisting.html). Here is an example of a persisting data class [BookStore](http://code.google.com/p/n-orm/source/browse/sample/src/main/java/com/googlecode/n_orm/sample/businessmodel/BookStore.java) taken from the [sample project](http://code.google.com/p/n-orm/source/browse/#hg%2Fsample).
```
@Persisting
public class BookStore {
	@Key private String name;
	private String address;

	public BookStore() {}
	
	public BookStore(String name) {
		this.name = name;
	}
}
```

The [Persisting annotation](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Persisting.html) makes the BookStore class automatically implement the [PersistingElement](http://code.google.com/p/n-orm/source/browse/storage/src/main/java/com/googlecode/n_orm/PersistingElement.java) interface plus all the necessary implementation as some kind of [mixin](http://en.wikipedia.org/wiki/Mixin) injection.

Data classes can inherit from each other. Be aware that the [Persisting annotation](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Persisting.html) is inherited.

All non static, non final, non transient, non [transient](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Transient.html)-annotated properties will be considered for storage. Possible types are simple types (int, String, ...),  data classes, non-data classes that only define attributes annotated with [Key](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Key.html), and arrays of such types. Maps and Sets of such types are also possible (see [below](#Column_families.md)).

Data classes automatically implement the [Comparable](http://download.oracle.com/javase/6/docs/api/java/lang/Comparable.html) and [Serializable](http://download.oracle.com/javase/6/docs/api/java/io/Serializable.html) interfaces.

# Keys to identify a persisting element #

The BookStore class defines an attribute name with a [Key annotation](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Key.html). A key is a mean to uniquely identify a data object. A data class must define at least one key. In case it defines more than one key, they must be explicitly ordered. In this latter situation, an object is identified according to values for **all** its keys, including keys for superclasses.

Another example is the [Book](http://code.google.com/p/n-orm/source/browse/sample/src/main/java/com/googlecode/n_orm/sample/businessmodel/Book.java) class, taken from the same project:
```
@Persisting
public class Book {
        @Key(order=1) private BookStore bookStore;
        @Key(order=2) private String title;
        private Date receptionDate;
        private short number;
	
	public Book() {}
	
	public Book(BookStore bookStore, String title) {
		this.bookStore = bookStore;
		this.title = title;
	}
}
```

Here, the Book data class defines N different keys giving each of them an order from 1 to N. Order number must not overlap, including in case of inheritance. One can see here that keys may have various natures: simple types (though reals are not supported), other data classes, classes with key-only attributes, and arrays of such types.

To get the actual identifier for an element, use the [getIdentifier](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/PersistingElement.html#getIdentifier()) method. To get an element whose identifier is known, proceed as follows
```
BookStore bs = new BookStore("cnaf");
String id = bs.getIdentifier();
```
```
BookStore bs = StorageManagement.getElement(BookStore.class, id);
```
Note that the identifier uses some non-printable characters as separators.

Persisting elements of the same class and with the same key are considered equivalent. [equals(Object)](http://download.oracle.com/javase/6/docs/api/java/lang/Object.html#equals(java.lang.Object)), [hashCode()](http://download.oracle.com/javase/6/docs/api/java/lang/Object.html#hashCode()), [toString()](http://download.oracle.com/javase/6/docs/api/java/lang/Object.html#toString()), and [compareTo(PersistingElement)](http://download.oracle.com/javase/6/docs/api/java/lang/Comparable.html#compareTo(T)) were implemented accordingly.

In case you want to reverse the natural order of elements, just use the [reversed](https://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Key.html#reverted()) property for the Key annotation. As an example, this feature will be useful in case you need to know an element with the latest possible date.

n-orm uses a cache to make correspond an identifier with an object. To use this cache, you can proceed as follows:
```
BookStore bs = StorageManagement.getElementWithKeys(BookStore.class, "cnaf");
```
or
```
BookStore bs = StorageManagement.getElementUsingCache(new BookStore("cnaf"));
```
or
```
BookStore bs = (BookStore)new BookStore("cnaf").getCachedVersion();
```
[More details](#Persisting_elements_cache.md) are available at the end of this article

# Storing, retrieving, searching, and processing #

To store such a persisting data object, just use the [store method](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/PersistingElement.html#store()):
```
BookStore bs = new BookStore("cnaf");
bs.setAddress("Turing str. 41");
bs.store();

Book b = new Book(bs, "n-orm for dummies");
b.setReceptionDate(new Date());
b.setNumber(10);
b.store();
```
To find back persisted elements, just re-create the object, and activate them so that they are updated with data from the data store:
```
BookStore bs = new BookStore("cnaf");
Book b = new Book(bs, "n-orm for dummies");
b.activate();
assert b.getNumber() == 10;
```

If you don't (want to) remember whether an object was activated or not but still want to avoid activating an already activated element, you can use [activateIfNotAlready()](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/PersistingElement.html#activateIfNotAlready(java.lang.String...)). In case you want an element to be activated if and only if not activated during the last X milliseconds, use [activateIfNotAlready(long)](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/PersistingElement.html#activateIfNotAlready(long,%20java.lang.String...)).
```
BookStore bs = new BookStore("cnaf");
Book b = new Book(bs, "n-orm for dummies");
b.activateIfNotAlready(); //Triggers a store request
assert b.getNumber() == 10;
...
b.activateIfNotAlready(); //Does not trigger a store request
```

This obviously means that you need to know key values. When it is not the case, you have to scan the data store. To efficiently do that, elements are stored in the data store in ascending order, class by class. To find elements, just indicate the expected class, key values (for all keys of order 1 to N where N <= M and M is the number of keys) and a maximal number of possible values:
```
//Finding all books from the book store
StorageManagement.findElements().ofClass(Book.class)
	.withKey("bookStore").setTo(bs)
	.withAtMost(10000).elements().go();
```
You can also search for elements between two keys, provided you explicit the expected values for keys of a lower [order](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Key.html#order()):
```
StorageManagement.findElements().ofClass(Book.class)
	.withKey("bookStore").setTo(bs)
	.withKey("title").between("M").and("N")
	.withAtMost(1000).elements().go();
```

Instead of the [go](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/query/SearchableClassConstraintBuilder.html#go()) method, you are encouraged to use the [iterate](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/query/SearchableClassConstraintBuilder.html#iterate()) and [forEach](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/query/SearchableClassConstraintBuilder.html#forEach(com.googlecode.n_orm.Process)) methods as elements won't be grabbed from the store immediately (which can be long and consume a lot of memory for large sets). Don't forget to [close](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/CloseableIterator.html#close()) the returned iterator... If you are looking for a precise element (i.e. you are looking for one - or zero - element), you can consider using [any](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/query/SearchableClassConstraintBuilder.html#any()).

By default, returned elements are not activated, and you need to activate them explicitly, thus triggering another store query for each element of the collection. Instead, you can use the [andActivate](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/query/SearchableClassConstraintBuilder.html#andActivate(java.lang.String...)) clause:

```
StorageManagement.findElements().ofClass(Book.class)
	.withKey("bookStore").setTo(bs)
	.withAtMost(10000).elements()
	andActivate().go();
```

If you just need to count the number of elements matching the query,  the [count](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/query/SearchableClassConstraintBuilder.html#count()) method is far more efficient:

```
StorageManagement.findElements().ofClass(Book.class)
	.withKey("bookStore").setTo(bs).count();
```

Range implies to be able to compare persisting elements between them. Sorting uses the natural sort for each one of the keys: number 0 is less than number 10, character string "abc" is less than "abcd" and "abd". To order persisting elements between them, their key are compared one by one using their order: if first key has same value for both persisting elements, then value for second key is compared and so on.

It is often the case that you need to perform a process on each element of a store. To do so, you are encouraged to use the [forEach](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/query/SearchableClassConstraintBuilder.html#forEach(com.googlecode.n_orm.Process,%20int,%20long)) method over a query. This method gives you the opportunity to set a timeout for processing, and to set a maximum parallel executing tasks, as shown below:
```
StorageManagement.findElements().ofClass(BookStore.class)
    .withAtMost(10000).elements()
    .forEach(new Process<BookStore>() {
                        @Override
                        public void process(BookStore element) {
                                element.delete();
                        }
                }, 10, 20000);
```
In that example, at most 10 000 book stores are deleted from the store using 10 parallel threads. Execution should be performed in less than 20 seconds unless an exception is thrown.

Sometimes, stores accept performing some code directly on the store server. This is the case for HBase. To invoke such a remote process, use the [remoteForEach](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/query/SearchableClassConstraintBuilder.html#remoteForEach(com.googlecode.n_orm.Process,%20com.googlecode.n_orm.Callback,%20int,%20long)) method instead:
```
WaitingCallBack wc = new WaitingCallBack();
StorageManagement.findElements().ofClass(Book.class)
    .withKey("bookStore").setTo(bssut)
    .withAtMost(10000).elements().andActivate()
    .remoteForEach(new BookSetter((short) 50), wc, 100, 1000);
wc.waitProcessCompleted();
```
In the above example, at most 10 000 books will be performed the BookSetter [process](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Process.html) by the store itself. In case store cannot support remote procedures in Java, [forEach](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/query/SearchableClassConstraintBuilder.html#forEach(com.googlecode.n_orm.Process,%20int,%20long)) is invoked asynchronously, using 100 parallel threads with a 10s timeout, but only in this latter case.

In contrary to [forEach](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/query/SearchableClassConstraintBuilder.html#forEach(com.googlecode.n_orm.Process,%20int,%20long)), [remoteForEach](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/query/SearchableClassConstraintBuilder.html#remoteForEach(com.googlecode.n_orm.Process,%20com.googlecode.n_orm.Callback,%20int,%20long)) is non blocking. To be told when process is ended, you can use a [callback](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Callback.html), which is able to tell if the process is ended, and what are the errors (if any). One simple implementation for the callback is the [WaitingCallBack](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/WaitingCallBack.html) that can wait for the process to be ended.


# Constraints on persisting classes #

## Constructors ##

In order to build up an object representing some piece of data in your data store (e.g. while using [forEach](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/query/SearchableClassConstraintBuilder.html#forEach(com.googlecode.n_orm.Process,%20int,%20long))), n-orm needs a constructor to be available. Two constructors can be used for this purpose, and at least one must be available:
  * default constructor (with no parameters)
  * keyed constructor.

_Default constructors_ are preferred. Then, in order to inject data from key, key attributes are populated using reflection.

A _keyed constructor_ is a constructor that takes all key types as parameter following the key order. Previous example showed a key constructor:

```
@Persisting
public class Book {
        @Key(order=1) private BookStore bookStore; // first key
        @Key(order=2) private String title; // second key
        private Date receptionDate;
        private short number;
	
	public Book(
                    BookStore bookStore /* for first key*/,
                    String title /* for second key */) {
		this.bookStore = bookStore;
		this.title = title;
	}
}
```

## Attributes ##

Attributes are simple properties which are neither [keys](#Keys_to_identify_a_persisting_element.md) nor [column families](#Column_families.md).

All non static, non final, non transient, non [transient](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Transient.html)-annotated properties will be considered for storage. Possible types are simple types (int, String, ...),  data classes, non-data classes that only define attributes annotated with [Key](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Key.html), and arrays of such types. Maps and Sets of such types are also possible (see [below](#Column_families.md)).

Only changed attributes are considered for being sent to store. Changed attributes are those attributes that got a value set since last time object was synchronized with data store (stored, activated or found - see [above)](#Storing,_retrieving,_searching,_and_processing.md), or since it was pre-initialized. By _pre-initialization_, we mean when object was created by the JVM, but **before** constructor is run (including supertype constructors). This means that any attribute whose value is set in the constructor, or whose value was set while declaring it is to be considered as changed next time data will be sent to data store. As an example:

```
@Persisting
public class BookStore {
	@Key private String name;
	public String address; 
        public Date creationDate = new Date(); // This is *bad* practice 

	public BookStore(String name) {
		this.name = name;
	}
}
```

If one want to change an existing BookStore instance, s/he may write
```
BookStore bs = new BookStore("cnaf");
bs.address = "1, Baker Str.";
bs.store()
```
Indeed, there is no need to activate `bs` before storing it.
The problem is that `bs.creationDate` was detected as changed since it was initialized after pre-initialization, and will also be sent to the data store, thus overriding the existing value for this property.

Instead, one should better write:

```
@Persisting
public class BookStore {
	@Key private String name;
	private String address; 
        public Date creationDate;

	public BookStore(String name) {
		this.name = name;
	}

	public String getAddress() {
		return this.address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public Date getCreationDate() {
		return this.creationDate == null ?
		new Date() : // the desired default value
		this.creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}
}
```

With the previous code
```
BookStore bs = new BookStore("cnaf");
bs.address = "1, Baker Str.";
bs.store()
```
will only send the new address to data store, leaving `creationDate` at its current state in the data store.

# Column families #

It is possible to define Map or Set attributes to represent [column families](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/storeapi/Store.html):
```
public static class Element {
	@Key public int anint;
	@Key(order=2) public String key;
	
	public Element(){}

	public Element(int anint, String key) {
		this.anint = anint;
		this.key = key;
	}
}

@Persisting public static class Container {
	private static final long serialVersionUID = 6300245095742806502L;
	@Key public String key;

	//Non persiting elements can be stored if they only define keys
	public Set<Element> elements = new HashSet<Element>(); //Can avoid being attributed by setting = null
	//Could also be declared
	//  public SetColumnFamily<Element> elements = null;
	// or
	//  public SetColumnFamily<Element> elements = new SetColumnFamily<Elements>();
	//  in order to access ColumnFamily features, and to be more efficient,
	//  but then Container is no longer serializable
	
	public Container() {}

	public Container(String key) {
		this.key = key;
	}
```
Here, Container.elements represents a column family as it is a Set (could have been a Map). Unlike [attributes](Attributes.md), column families **must be set in constructors**. Morevover, **column families must not be set outside constructors.**
Column families can contain persisting elements, simple elements (String, int, Byte, ...) and other objects whose class contains attributes all marked `@Key`. In the latter case, contained (be them keys or values) elements data are fully contained in their key and don't need to be persisting.

Storing column families is transparent:
```
Container sut = new Container("key");
for(int i = 1 ; i <= 10; ++i) {
	sut.elements.add(new Element("E" + i));
}
sut.store();
```

Activating a column family for a persisting element must be done explicitly  to avoid unnecessary activation of numerous elements:
```
Container copy = new Container(sut.key);
copy.activate("elements"); //Also activates simple properties
assert sut.elements.size() == copy.elements.size();
for(Element e : sut.elements)
	assert copy.elements.contains(e);
```
You can also activate only a column family without also activating simple properties, [with](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/PersistingElement.html#activateColumnFamily(java.lang.String,%20java.lang.Object,%20java.lang.Object)) or [without](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/PersistingElement.html#activateColumnFamily(java.lang.String)) a constraint on the range of elements to be activated.

Another possibility, is to use the [Implicit annotation](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/ImplicitActivation.html) while declaring the column familiy:
```
@Persisting public static class Container {
	@ImplicitActivation public Set<Element> elements = ...
}
```
```
Container copy = new Container(sut.key);
copy.activate(); //no need to state activate("elements")
assert sut.elements.size() == copy.elements.size();
for(Element e : sut.elements)
	assert copy.elements.contains(e);
```

## Modeling multiple associations ##

In case one persisting element belongs to another persisting element (e.g. a Book belonging to a BookStore), you might not always want to create a column family. Instead, try to refer to the owner BookStore from the Book as its first-order key as it is done in the [Sample project](http://code.google.com/p/n-orm/source/browse/sample/src/main/java/com/googlecode/n_orm/sample/businessmodel/Book.java). To find the owned elements, you can then perform a search on the datastore (see [Bookstore.getBooks()](http://code.google.com/p/n-orm/source/browse/sample/src/main/java/com/googlecode/n_orm/sample/businessmodel/BookStore.java) operation).

More generally, in case you have a 1/N relationship, prefer setting a key to handling a column family as key search is better managed than column families in most data stores.

# Incrementing properties #

In case you want to have counter-like properties, you will certainly need to write something like this:
```
@Persisting
public class Book {
    @Key private String isbn;
    private int inStock;
    
    public void newBook() {
        this.inStock++;
    }

    public void bookSold() {
        this.inStock--;
    }
}

...
myBook.activate();
myBook.newBook();
myBook.save();
```

A problem with this approach is that you have to think activating your book each time you set such property, which is error-prone, and triggers an extra call to the data store. Even worse, if another process performs a store with an updated value for `inStock` between the `activate()` and `store()` calls, updated value will be false.

Instead, you can declare the `inStock` property as [incrementing](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/Incrementing.html). This makes the property be stored not with its actual value, but with an increment value (i.e. the difference between the value at `store()` time and the value at the previous `activate()`, `store()`, or 0). As a consequence, there is no longer a need for activating the object before any property set. New version for the book class becomes:

```
@Persisting
public class Book {
    @Key private String isbn;
    @Incrementing private int inStock;
    
    public void newBook() {
        this.inStock++;
    }

    public void bookSold() {
        this.inStock--;
    }
}

...
myBook.newBook();
myBook.save();
```

Map column families can also be marked as incrementing. In this case, values for elements are sent to their store using an increment rather than sending their absolute value.

By default, incrementing properties can only increment over time (and thus must be positive !). You can change this behavior by stating the mode:
```
@Persisting
public class Book {
    @Key private String isbn;
    //inStock must be able to increment AND decrement
    @Incrementing(mode=Incrementing.Mode.Free) private int inStock;
}
```

Nevertheless, incrementing properties have to be used with care, as examplified below:
```
Book b1 = ...;
assert b1.inStore = 0;
b1.store();
Book b2 = new Book();
b2.isbn = b1.isbn; //Two java objects representing the same data entity as they have same key
b1.inStock = 3;
b2.inStock = 4;
b1.store(); b2.store();
b1.activate(); b2.activate();
assert b1.inStock = 7;
assert b2.inStock = 7;
```

## Checking incrementing properties ##

Incrementing properties are checked at runtime as soon as the property is incremented so that one can know where the illegal de/increment happens, e.g. `b1.inStock--`immediately raises an [Incrementing](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/IncrementException.html) exception. However, this immediate check might be costly to check in case you often increment a property.

You can disable immediate check using `IncrementManagement.setImmedatePropertyCheck(false);`.

Immediate check is only available on properties. Regarding column families, it is performed immediately if you instanciate `new SetColumnFamily<?>()`or `new MapColumnFamily<?>()`, regardless of the value for `IncrementManagement.immedatePropertyCheck`.

# Activation and storage of a graph of persisting elements #

A persisting element is often related to other persisting elements. When a persisting element is stored or activated, all its dependencies can also stored or activated (transitively) in the following way:
```
@Persisting
public class Book {
    @Key @ImplicitActivation public BookStore bookStore;
}
```

```
BookStore bs = new BookStore("cnaf");
Book b = new Book(bs, "n-orm for dummies");

//bs is also activated automatically
assert bs.getAddress() == null;
b.activate();
assert bs.getAddress().equals("Turing str. 41");
```

All properties whose type is a data class can be activated this way.
Elements in a column family (i.e. in a Set or a Map) can also be automatically activated this way.

# Persisting elements cache #

In order for cyclic graph not to indefinitely create new object while activating them, a [cache](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/cache/Cache.html) of elements is maintained for each one of your threads. A side-effect for this is the following:
```
BookStore bs1 = new BookStore("cnaf");
BookStore bs2 = StorageManagement.getElement(BookStore.class, bs1.getIdentifier()); //or a search result
assert bs1 == bs2;
```

As it is a per-thread cache, you don't need to think of thread safety unless you share persisting elements between threads. In the latter case, you must protect you store and activation calls with a synchronized section either on the stored / activated object, or on the stored / activated column family.

In order to share elements between threads, you can try to explicitly put an element E into a thread's cache by calling the following code from the destination thread before using the element E:
```
KeyManagement.getInstance().register(E);
```

By default, at most 10 000 elements can reside in a cache during at most 10s. Of course, you can [configure](http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/cache/Cache.html) those values.