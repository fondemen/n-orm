package com.mt.storage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Persisting {
	/**
	 * The name of the table where should be stored instances.
	 * If let empty (default value), the name of the class is used.
	 */
	String table() default "";
	
	/**
	 * States whether key values should be stored in the property column family.
	 * The normal case is that keys are encoded in identifier of the instances.
	 */
	boolean storeKeys() default false;
	
	/**
	 * States whether values (properties and column familes) should also be stored in superclasses.
	 * The normal case is not as information is stored in the table for the instance's class already.
	 */
	boolean storeAlsoInSuperClasses() default false;
}
