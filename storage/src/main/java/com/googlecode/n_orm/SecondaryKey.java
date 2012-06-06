package com.googlecode.n_orm;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines a java attribute as participating in a secondary key so that a {@link PersistingElement} annotated with {@link SecondaryKeys} can be searched not only according to primary keys, but also using another set of keys.<br>
 * A secondary key must be declared on the persisting elements classes from which they can be searched using {@link SecondaryKeys}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface SecondaryKey {
	/**
	 * The secondary key in which the annotated attribute participates following the pattern
	 * <i>name</i>::<i>order</i> (or merely <i>name</i> in case order is 1)<br>
	 * To reverse the key, use 
	 * <i>name</i>::<i>order</i>#reverted (or merely <i>name</i>#reverted in case order is 1)
	 */
	public String[] value();
}
