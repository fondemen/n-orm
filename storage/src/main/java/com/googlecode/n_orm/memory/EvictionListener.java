package com.googlecode.n_orm.memory;

/**
 * Interface for eviction listeners.
 * To be used with {@link EvictionQueue#addEvictionListener(EvictionListener)}
 * or merely {@link Memory#addEvictionListener(EvictionListener)}.
 *
 */
public interface EvictionListener {
	/**
	 * Method called after a row is evicted.
	 * In case of a shutdown (JVM stop), best effort is made for that
	 *  all planned rows are evicted, even those registered before now.
	 */
	void rowEvicted(com.googlecode.n_orm.storeapi.Row row);
}