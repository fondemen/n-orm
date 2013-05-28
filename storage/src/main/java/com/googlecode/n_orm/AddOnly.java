package com.googlecode.n_orm;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation may be placed on a property representing a column family
 * (i.e. a {@link java.util.Map} or a {@link java.util.Set}) to prevent an
 * element's removal from the collection. In case a remove happens, an
 * {@link IllegalStateException} is thrown.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface AddOnly {

}
