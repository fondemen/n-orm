package com.googlecode.n_orm.cache.read;

import java.util.Map;

import com.googlecode.n_orm.storeapi.MetaInformation;

/**
 * Defines what a cache is.
 */
public interface ICache {
	/**
	 * delete an element in the cache using the key
	 */
	public void delete(MetaInformation meta, String table, String key) throws CacheException;
	/**
	 * To insert an element in the cache
	 */
	public void insertFamilyData(MetaInformation meta, String table, String key, String family, Map<String, byte[]> familyData) throws CacheException;
	/**
	 * Return an element of the cache using  the key
	 */
	public Map<String, byte[]> getFamilyData(MetaInformation meta, String table, String key,String family) throws CacheException;
	/**
	 * return the the approximate number of entries in the cache
	 */
	public long size() throws CacheException;
	/**
	 * delete all the element in the cache
	 */
	public void reset() throws CacheException;
	/**
	 * returns the maximum number of elements that can contain cache
	 */
	public long getMaximunSize() throws CacheException;
	/**
	 * Change the maximum of elements that can contain cache
	 */
	public void setMaximunSize(int size) throws CacheException;
	/**
	 * returns the life of elements contained in the cache
	 */
	public long getTTL() throws CacheException;
	/**
	 * for change the life of element in the cache
	 */
	public void setTTL(long TTL) throws CacheException;
	public boolean existsData(MetaInformation meta, String table, String id,
			String family) throws CacheException;

}
