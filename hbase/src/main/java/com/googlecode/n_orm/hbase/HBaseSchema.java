package com.googlecode.n_orm.hbase;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.cf.ColumnFamily;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD})
@Inherited
/**
 * Alters default values for a particular {@link Persisting} class or one of its column families.
 * Values defined by this annotation are just ignored in case the store for this element is not targeting HBase (see {@link PersistingElement#getStore()}).
 * Unless stated, values provided by this annotation have higher priority than their equivalent in {@link Store}.
 * Values for a column family have higher priority than values for a persisting element.
 */
public @interface HBaseSchema {
	public static enum SettableBoolean {
		/**
		 * A value to state that the set property should not be considered
		 */
		UNSET,
		TRUE,
		FALSE};
	
	/**
	 * Changes value for {@link Store#isInMemory()} for a particular persisting class or column family.
	 * WARNING: to be used with great care in case of use in multiprocess context.
	 */
	SettableBoolean forceCompression() default SettableBoolean.UNSET;

	/**
	 * Changes value for {@link Store#getCompression()} for a particular persisting class or column family.
	 * Value "" or describing an unknown compressor is equivalent to an unset value.
	 */
	String compression() default "";
	
	/**
	 * Sets a default value for {@link Store#getScanCaching()} for this {@link PersistingElement} or {@link ColumnFamily}.
	 * Value less or equal than 0 is equivalent to an unset value.
	 */
	int scanCaching() default -1;

	/**
	 * Changes value for {@link Store#isForceInMemory()} for a particular persisting class or column family.
	 * WARNING: to be used with great care in case of use in multiprocess context.
	 */
	SettableBoolean forceInMemory() default SettableBoolean.UNSET;
	/**
	 * Changes value for {@link Store#isInMemory()} for a particular persisting class or column family.
	 */
	SettableBoolean inMemory() default SettableBoolean.UNSET;
}
