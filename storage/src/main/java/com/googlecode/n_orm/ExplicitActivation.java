package com.googlecode.n_orm;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.PersistingElement;

/**
 * Prevents automatic activation of a property (including keys).
 * Persisting properties (including keys) are automatically activated while using {@link PersistingElement#activate(String...)}.
 * By marking a property with this annotation, an activation will not automatically trigger an activation.
 * It is not useful to declare this annotation on a column family for they will not be automatically be activated anyway unless annotated with {@link ImplicitActivation}.
 * @see PersistingElement#activate(String...)
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ExplicitActivation {

}
