package com.googlecode.n_orm;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines a java attribute as an index so that a {@link PersistingElement} annotated with {@link Indexable} can be searched not only according to keys.<br>
 * An index must be declared on the persisting elements classes from which they can be searched using {@link Indexable()}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Index {
	/**
	 * The indexes in which the annotated attribute participates following the pattern
	 * <i>indexname</i>::<i>indexorder</i> (or merely <i>indexname</i> in case order is 1)
	 */
	public String[] value();
}
