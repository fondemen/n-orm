package com.googlecode.n_orm.hbase;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD})
@Inherited
/**
 * Alters default values for a particular {@link Persisting} class or one of its column families.
 * VAlues defined by this annotation are just ignored in case the store for this element is not targeting HBase (see {@link PersistingElement#getStore()}). 
 */
public @interface HBaseSchema {
	public static enum SettableBoolean {
		/**
		 * A value to state that the set property should not be considered as an overload
		 */
		UNSET,
		TRUE,
		FALSE};
	
	/**
	 * Changes value for {@link Store#isInMemory()} for a particular persisting class or column family.
	 */
	SettableBoolean forceCompression() default SettableBoolean.UNSET;

	/**
	 * Changes value for {@link Store#getCompression()} for a particular persisting class or column family.
	 * Value "" is equivalent to an unset value.
	 */
	String compression() default "";
	
	int scanCaching() default -1;

	/**
	 * Changes value for {@link Store#isForceInMemory()} for a particular persisting class or column family.
	 */
	SettableBoolean forceInMemory() default SettableBoolean.UNSET;
	/**
	 * Changes value for {@link Store#isInMemory()} for a particular persisting class or column family.
	 */
	SettableBoolean inMemory() default SettableBoolean.UNSET;
}
