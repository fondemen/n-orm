package com.googlecode.n_orm.cache.read;
import com.googlecode.n_orm.cache.read.CacheException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.cache.read.ICache;
import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.DelegatingStore;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Store;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;

public abstract class CachedStore extends DelegatingStore{
	private ICache cache;
	

	public CachedStore(Store actualStore, ICache cache) {
		super(actualStore);
		this.cache = cache;	
	}
	
	public void delete(MetaInformation meta, String table, String id) 
			throws DatabaseNotReachedException {
		Collection<ColumnFamily<?>> cfs = meta.getElement().getColumnFamilies();
		String family=cfs.getClass().getName();
		try {
			if(cache.existsData(meta, table, id, family)){
					cache.delete(meta, table, id);}
			else{
				System.out.println("No Data To Delete!");
				}
		} catch (CacheException e) {
			new DatabaseNotReachedException("The element can'nt be delete");
		}
			
	}
	/**
	 * check if an element exist in the cache
	 */
	public boolean exists(MetaInformation meta, String table, String row,
			String family) throws DatabaseNotReachedException {
		String key=table.concat(row).concat(family);
		try {
			return cache.existsData(meta, table, row, family);
		} catch (CacheException e) {
			new DatabaseNotReachedException("");
		}
		return false;
	}
	/*
	 * Check if an element is in the cache or in the store.
	 * @see com.googlecode.n_orm.storeapi.DelegatingStore#get(com.googlecode.n_orm.storeapi.MetaInformation, java.lang.String, java.lang.String, java.lang.String)
	 */
	public Map<String, byte[]> get(MetaInformation meta, String table,
			String id, String family) throws DatabaseNotReachedException {
		try{
		Map<String, byte[]> data= cache.getFamilyData(meta, table,id, family);
		if(data!=null){
			return data;
			}
		else{
			Map<String, byte[]> familyData=getActualStore().get(meta, table, id, family);
			cache.insertFamilyData(meta, table, id, family, familyData);
				assert familyData.equals(cache.getFamilyData(meta, table,id, family));
				return familyData;
			}
		} catch(CacheException e){
			new DatabaseNotReachedException("No family Data");
		}
		return null;
	}
	/**
	 * Check if a collection of element exist in the cache
	 */
	
	public boolean exists(MetaInformation meta, String table, String row)
			throws DatabaseNotReachedException {
		Collection<ColumnFamily<?>> cfs = meta.getElement().getColumnFamilies();
		for (ColumnFamily<?> columnFamily : cfs){
			String name=columnFamily.getName();
			try {
				if(cache.existsData(meta, table,row, name)){
					return true;
				 }
				else{
					return false;
					}
			} catch (CacheException e) {
				new DatabaseNotReachedException("The element doesn't exist in the cache");
			}
			}
		return false;
		}
	
	public ColumnFamilyData get(MetaInformation meta, String table, String id,
			Set<String> families) throws DatabaseNotReachedException {
		try {
			DefaultColumnFamilyData dcfd=new DefaultColumnFamilyData();
			Set<String> familiesName=new TreeSet<String>(families);
			Iterator it=familiesName.iterator();
			String name=null;
			Map<String, byte[]> data=new HashMap<String, byte[]>();
			
			while(it.hasNext()){
				name=(String)it.next();
				 data = cache.getFamilyData(meta, table, id, name);
				if(data!=null){
					dcfd.put(name, data);
					it.remove();
				}
			}
			ColumnFamilyData dataStore = getActualStore().get(meta, table, id, familiesName);
			
			for(Entry<String, Map<String, byte[]>> cfd: dataStore.entrySet()){
				dcfd.put(cfd.getKey(), cfd.getValue());
				cache.insertFamilyData(meta, table, id, cfd.getKey(), cfd.getValue());
			}
			return dcfd;
		} catch (CacheException e) {
			new DatabaseNotReachedException("");
		}
		return null;
	}
}

