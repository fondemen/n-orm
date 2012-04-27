package com.googlecode.n_orm;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declare indexes on an {@link PersistingElement} so that it can be searched not only according to keys, but also according to attibutes annotated with {@link Index}.<br>
 * Indexes are stored in separate tables whose name will be <i>class table</i>.<i>index name</i> and will be updated
 * each time a {@link PersistingElement#store()} is invoked (REM: transactions are not supported), so it's better to avoid defining too many or useless indexes.<br>
 * Indexes are just like {@link Key} except that they have a name. Several indexes can be specified on a same attribute.
 * An index is given following the pattern <i>indexname</i>#<i>indexorder</i>, or merely <i>indexname</i> in case order is 1.
 * In case the index should be reverted, the pattern becomes <i>indexname</i>#<i>indexorder</i>#reverted (or <i>indexname</i>#reverted in case order is 1).<br>
 * Here is an example placing two indexes <code>firstindex</code> and <code>secondindex</code> on a class:<br><code>
 *  &#64;Persisting &#64;Indexable(indexes={"fisrtindex", "secondindex"}) public class Element {<br>
 * &nbsp;&nbsp; &#64;Key &#64;Indexing({"secondindex#2#reverted"}) Private String key;<br>
 * &nbsp;&nbsp; &#64;Indexing({"firstindex#1", "secondindex#2"}) private String att1;<br>
 * &nbsp;&nbsp; &#64;Indexing({"firstindex#2"}) private String att2; <br>
 * }
 * </code><br>
 * As such, instances of <code>Element</code> can be searched not only according to ist key <code>key</code>,
 * but also either according to <code>att1</code> and/or <code>att2</code> (according to index <code>firstindex</code>),
 * or according to <code>att1</code> and/or <code>key</code> (according to index <code>secondindex</code>).<br>
 * As an example, an element can be searched the following way:<br><code>
 * StorageManagement.findElements().ofClass(Element.class).withIndex("firstindex")<br>
 * .withKey("att1").setTo("annatt1value")<br>
 * .andWithKey("key").lessOrEqualsThan("anupperkeyvalue")<br>
 * .withAtMost(1000).elements().go()</code>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Indexable {

	/**
	 * The list of indexes with which this class can be searched (see {@link Index})
	 */
	String[] value();

}
