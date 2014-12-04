package com.googlecode.n_orm.hbase;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD})
@Inherited
/**
 * Alters default values for a particular {@link com.googlecode.n_orm.Persisting} class or one of its column families.
 * Values defined by this annotation are just ignored in case the store for this element is not targeting HBase (see {@link com.googlecode.n_orm.PersistingElement#getStore()}).
 * Unless stated, values provided by this annotation have higher priority than their equivalent in {@link Store}.
 * Values for a column family have higher priority than values for a persisting element.
 */
public @interface HBaseSchema {
	public static enum SettableBoolean {
		/**
		 * A value to state that the set property should not be considered and rather found from store (or class-level {@link HBaseSchema} in case this annotation annotated a column family)
		 */
		UNSET,
		TRUE,
		FALSE};
		
	public static enum WALWritePolicy {
		/** No explicit policy set. By default, equivalent to {@link #USE}. */
		UNSET((byte)63),
		/** WAL should not be used at all. */
		SKIP((byte)0),
		/** WAL should be used asynchronously. */ 
		ASYNC((byte)32),
		/** WAL should be used synchronously. */ 
		USE((byte)64);
		
		private int strength;
		
		private WALWritePolicy(byte strength) {
			this.strength = strength;
		}
		
		public boolean strongerThan(WALWritePolicy rhs) {
			return this.strength > rhs.strength;
		}
	}
	
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
	 * Sets a default value for {@link Store#getScanCaching()} for this {@link com.googlecode.n_orm.PersistingElement} or {@link com.googlecode.n_orm.cf.ColumnFamily}.
	 * Value less or equal than 0 is equivalent to an unset value.
	 * In case different column families are read at same time, the lowest value is taken.
	 * In case a value is defined at family level for at least one of the activated families (see {@link com.googlecode.n_orm.PersistingElement#activate(String...)}), class- and store-level scan caching is ignored.
	 */
	int scanCaching() default -1;
	
	/**
	 * Whether or not to use the Write Away Log (WAL) while storing this {@link com.googlecode.n_orm.PersistingElement} (default is true).
	 * In case more than one column family are implied in a store, {@link WALWritePolicy#strongerThan(WALWritePolicy) strongest} policy 
	 * is used for the complete store.
	 */
	WALWritePolicy writeToWAL() default WALWritePolicy.UNSET;

	/**
	 * Changes value for {@link Store#isForceInMemory()} for a particular persisting class or column family.
	 * WARNING: to be used with great care in case of use in multiprocess context.
	 */
	SettableBoolean forceInMemory() default SettableBoolean.UNSET;
	/**
	 * Changes value for {@link Store#isInMemory()} for a particular persisting class or column family.
	 */
	SettableBoolean inMemory() default SettableBoolean.UNSET;

	/**
	 * Changes value for {@link Store#isForceTimeToLive()} for a particular persisting class or column family.
	 * WARNING: to be used with great care in case of use in multiprocess context.
	 */
	SettableBoolean forceTimeToLive() default SettableBoolean.UNSET;
	/**
	 * Changes value for {@link Store#getTimeToLiveSeconds()} for a particular persisting class or column family.
	 */
	int timeToLiveInSeconds() default -1;

	/**
	 * Changes value for {@link Store#isForceMaxVersions()} for a particular persisting class or column family.
	 * WARNING: to be used with great care in case of use in multiprocess context.
	 */
	SettableBoolean forceMaxVersions() default SettableBoolean.UNSET;
	/**
	 * Changes value for {@link Store#getMaxVersions()} for a particular persisting class or column family.
	 */
	int maxVersions() default -1;
	
	/**
	 * Changes value for {@link Store#isForceBloomFilterType()} for a particular persisting class or column family.
	 * WARNING: to be used with great care in case of use in multiprocess context.
	 */
	SettableBoolean forceBloomFilterType() default SettableBoolean.UNSET;
	/**
	 * Changes value for {@link Store#getMaxVersions()} for a particular persisting class or column family.
	 * Possible values are those from {@link org.apache.hadoop.hbase.regionserver.StoreFile.BloomType}, e.g. "NONE" (default), "ROW", or "ROWCOL"
	 */
	String bloomFilterType() default "";

	/**
	 * Changes value for {@link Store#isForceBlockCacheEnabled()} for a particular persisting class or column family.
	 * WARNING: to be used with great care in case of use in multiprocess context.
	 */
	SettableBoolean forceBlockCacheEnabled() default SettableBoolean.UNSET;
	/**
	 * Changes value for {@link Store#getBlockCacheEnabled()} for a particular persisting class or column family.
	 */
	SettableBoolean blockCacheEnabled() default SettableBoolean.UNSET;

	/**
	 * Changes value for {@link Store#isForceBlockSize()} for a particular persisting class or column family.
	 * WARNING: to be used with great care in case of use in multiprocess context.
	 */
	SettableBoolean forceBlockSize() default SettableBoolean.UNSET;
	/**
	 * Changes value for {@link Store#getBlockSize()} for a particular persisting class or column family.
	 * Value less or equal than 0 is equivalent to an unset value.
	 */
	int blockSize() default -1;

	/**
	 * Changes value for {@link Store#isForceReplicationScope()} for a particular persisting class or column family.
	 * WARNING: to be used with great care in case of use in multiprocess context.
	 */
	SettableBoolean forceReplicationScope() default SettableBoolean.UNSET;
	/**
	 * Changes value for {@link Store#getReplicationScope()} for a particular persisting class or column family.
	 */
	int replicationScope() default -1;
	
	/**
	 * Changes value for {@link Store#getDeferredLogFlush()} for a particular persisting class (ignored on column families).
	 */
	SettableBoolean deferredLogFlush() default SettableBoolean.UNSET;

	/**
	 * Changes value for {@link Store#isForceDeferredLogFlush()} for a particular persisting class (ignored on column families).
	 * WARNING: to be used with great care in case of use in multiprocess context.
	 */
	SettableBoolean forceDeferredLogFlush() default SettableBoolean.UNSET;
}
