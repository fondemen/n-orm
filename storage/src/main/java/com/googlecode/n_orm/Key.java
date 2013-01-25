package com.googlecode.n_orm;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Date;


/**
 * Defines a java attribute as a key.
 * Keys have constraint on their type as defined in documentation for {@link Persisting}. Double and float are not supported.
 * {@link Persisting} elements must define at least one key.
 * Non {@link Persisting} elements that have to be persisted in the store (e.g. as property value or in a column family as an index or a value) must define only keys properties.
 * In case a class defines more than one key, those keys must be given an incrementing {@link #order()}. First order is 1.
 * Instances of such classes can be represented and rebuilt by a {@link String} using {@link com.googlecode.n_orm.conversion.ConversionTools#convertFromString(Class, String)} and {@link com.googlecode.n_orm.conversion.ConversionTools#convertToString(Object)}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Key {
	/**
	 * The order of the element that must differ from order of other keys.
	 */
	public int order() default 1;
	
	/**
	 * Whether the key should be stored in an inverted way ; supported types are integer (long, {@link Long}, etc), boolean, and {@link Date}.
	 * A search is always performed according to a start key and continues using ascending order ({@link StorageManagement#findElements()}).
	 * Inverting a key changes the order of elements.<br>
	 * An example is {@link java.util.Date} keys, as in the following class:<br>
	 * <code>public class Foo { @Key public Date bar; }</code><br>
	 * Searching Foos like in <code>StorageManagement.findElements().ofClass(FooStd.class).withKey("bar").greaterOrEqualsThan(d).withAtMost(100).elements().iterate()</code>
	 * will iterate over elements whose key <code>bar</code> is later or equals than date <code>d</code>. Reverting the key, those elements would be elements earlier or equal than <code>d</code>.
	 */
	public boolean reverted() default false;
}
