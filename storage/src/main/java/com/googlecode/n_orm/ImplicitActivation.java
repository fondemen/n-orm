package com.googlecode.n_orm;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.ImplicitActivation;
import com.googlecode.n_orm.PersistingElement;


/**
 * When applied to a property whose type is a persisting class, makes the property be activated automatically after the owner persisting element is activated using {@link PersistingElement#activateIfNotAlready(String...)}.
 * When applied to a column family (i.e. properties of type {@link Map} or {@link Set}), makes it systematically be activated using {@link PersistingElement#activate(String...)}.
 * However, be careful with this annotation for all elements in the column family will be activated regardless their number.
 * In any case, it issues the data store more requests, so it is to be used with care.
 * @see PersistingElement#activate(String...)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ImplicitActivation {
}
