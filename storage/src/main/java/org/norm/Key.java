package org.norm;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.norm.conversion.ConversionTools;

/**
 * Defines a java attribute as a key.
 * Keys have constraint on their type as defined in documentation for {@link Persisting}. Double and float are not supported.
 * {@link Persisting} elements must define at least one key.
 * Non {@link Persisting} elements that have to be persisted in the store (e.g. as property value or in a column family as an index or a value) must define only keys properties.
 * In case a class defines more than one key, those keys must be given an incrementing {@link #order()}. First order is 1.
 * Instances of such classes can be represented and rebuilt by a {@link String} using {@link ConversionTools#convertFromString(Class, String)} and {@link ConversionTools#convertToString(Object)}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Key {
	/**
	 * The order of the element that must differ from order of other keys.
	 */
	public int order() default 1;
}
