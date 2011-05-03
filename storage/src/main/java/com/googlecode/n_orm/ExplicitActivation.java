package com.googlecode.n_orm;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.PersistingElement;

/**
 * Column families (i.e. properties of type {@link Map} or {@link Set}) must be activated explicitly using {@link PersistingElement#activate(String...)}.
 * By marking a column family with this annotation, an activation will always trigger an activation. However, be careful with this annotation for all elements in the column family will be activated regardless their number.
 * It is not useful to declare this annotation on a normal property or a key (including persisting properties or key) for they will be automatocally be activated.
 * @see PersistingElement#activate(String...)
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ExplicitActivation {

}
