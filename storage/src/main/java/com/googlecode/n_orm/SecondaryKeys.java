package com.googlecode.n_orm;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declare a secondary key set on a {@link PersistingElement} so that it can be searched not only according to keys, but also according to attributes annotated with {@link SecondaryKey}.<br>
 * Secondary keys are stored in separate tables whose name will be <i>class table</i>.<i>secondary key name</i> and will be updated
 * each time a {@link PersistingElement#store()} is invoked (REM: transactions are not supported), so it's better to avoid defining too many or useless secondary keys.
 * Legacy data will not be indexed according to secondary keys if stored before the secondary keys are defined.<br>
 * Secondary keys are just like {@link Key}s except that they have a name, e.g. their value cannot change. A same attribute can participate in any number of secondary keys, be it already a key or not.
 * A secondary key is given following the pattern <i>name</i>#<i>order</i>, or merely <i>name</i> in case order is 1.
 * In case the secondary key should be reverted, the pattern becomes <i>name</i>#<i>order</i>#reverted (or <i>name</i>#reverted in case order is 1).<br>
 * Here is an example placing two secondaty keys <code>firssk</code> and <code>secondsk</code> on a class:<br><code>
 *  &#64;Persisting &#64;SecondaryKeys({"fisrtsk", "secondsk"}) public class Element {<br>
 * &nbsp;&nbsp; &#64;Key &#64;SecondaryKey({"secondsk#2#reverted"}) Private String key;<br>
 * &nbsp;&nbsp; &#64;SecondaryKey({"firstsk#1", "secondsk#2"}) private String att1;<br>
 * &nbsp;&nbsp; &#64;SecondaryKey({"firstsk#2"}) private String att2; <br>
 * }
 * </code><br>
 * As such, instances of <code>Element</code> can be searched not only according to its key <code>key</code>,
 * but also either according to <code>att1</code> and/or <code>att2</code> (according to secondary key <code>firstsk</code>),
 * or according to <code>att1</code> and/or <code>key</code> (according to secondary key <code>secondsk</code>).<br>
 * As an example, an element can be searched the following way:<br><code>
 * StorageManagement.findElements().ofClass(Element.class).withSecondaryKey("firstsk")<br>
 * .withKey("att1").setTo("annatt1value")<br>
 * .andWithKey("key").lessOrEqualsThan("anupperkeyvalue")<br>
 * .withAtMost(1000).elements().go()</code><br>
 * This annotation is separated from {@link Persisting} as it can be declared across different levels of inheritance, including on non {@link Persisting}-annotated elements.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SecondaryKeys {

	/**
	 * The list of secondary keys with which this class can be searched (see {@link SecondaryKey})
	 */
	String[] value();

}
