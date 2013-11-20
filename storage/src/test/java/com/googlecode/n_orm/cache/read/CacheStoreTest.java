package com.googlecode.n_orm.cache.read;
import static org.junit.Assert.*;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.cache.read.CacheException;
import com.googlecode.n_orm.cache.read.ICache;
import static org.easymock.EasyMock.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.Store;


public class CacheStoreTest {
	
	@Persisting
	public static class Element {
		private static final long serialVersionUID = 571650039153672799L;
		@Key public String key;
		public String familyName;
		public Map<String, Integer> familyData = null;
		
	}
	
	private Store mockStore;
	private ICache mockCache;
	private CachedStore sut;

	@Before
	public void setUp() throws Exception {
		mockStore=createMock(Store.class);
		mockCache=createMock(com.googlecode.n_orm.cache.read.ICache.class);
		sut=new CachedStore(mockStore,mockCache);
		
	}
	
	private void verify() {
		EasyMock.verify(mockStore,mockCache);
	}
	private void replay() {
		EasyMock.replay(mockStore,mockCache);
	}
	
	@Test
	public void testGet() throws CacheException {
		expect(mockCache.getFamilyData(null, "table", "key", "family")).andReturn(null);
		Map<String, byte[]> familyData=new HashMap<String,byte[]>();
		byte[] Dupond = ConversionTools.convert("Dupont");
		byte[] Jean = null;
		byte[] age= ConversionTools.convert("10");
		familyData.put("nom", Dupond);
		familyData.put("Prenom", Jean);
		familyData.put("age", age);
		expect(mockStore.get(null, "table", "key", "family")).andReturn(familyData);
		mockCache.insertFamilyData(null, "table", "key", "family", familyData);
		replay();
		sut.get(null, "table", "key", "family");
		verify();
		
	}
	
	@Test
	public void testExist() throws CacheException{
		Element e=new Element();
		e.key="tagada";
		e.familyName="tsoin tsoin";
		e.familyData .put("A", 12);
		e.familyData.put("B", 14);
		MetaInformation meta=new MetaInformation();
		expect(mockCache.existsData(meta, "table", "key", "family")).andReturn(true);
		replay();
		assertTrue(sut.exists(meta, "table", "key", "family"));
		verify();
	}
	
	@Test
	public void testNotExist() throws CacheException{
		Element e=new Element();
		e.key="tagada";
		e.familyName="tsoin tsoin";
		e.familyData .put("A", 12);
		e.familyData.put("B", 14);
		MetaInformation meta=new MetaInformation();
		expect(mockCache.existsData(meta, "table", "key", "family")).andReturn(false);
		expect(sut.exists(meta, "table", "key")).andReturn(false);
		replay();
		assertFalse(sut.exists(meta, "table", "key", "family"));
		verify();
	}
	
	@Test
	public void testDelete()throws CacheException{
		Element e=new Element();
		e.key="tagada";
		e.familyName="tsoin tsoin";
		e.familyData.put("A", 12);
		e.familyData.put("B", 14);
		MetaInformation meta=new MetaInformation();
		mockCache.delete(meta, "table", "key");
		mockStore.delete(meta, "table", "key");
		replay();
		sut.delete(meta, "table", "key");
		verify();
	}
	
	@Test
	public void testExists() throws CacheException{
		Element e=new Element();
		e.key="tagada";
		e.familyName="tsoin tsoin";
		e.familyData.put("A", 12);
		e.familyData.put("B", 14);
		MetaInformation meta=new MetaInformation();
		meta.forElement(new Element());
		expect(mockCache.existsData(eq(meta), eq("table"), eq("row"), EasyMock.or(eq("props"), eq("familyData")))).andReturn(true);
		replay();
		sut.exists(meta, "table", "row");
		verify();
	}
	
	@Test
	public void testDataStore() throws CacheException/*All data are from the store*/{
		Set<String> storedFamilies=new HashSet<String>();
		storedFamilies.add("f1Store");
		storedFamilies.add("f2Store");
		storedFamilies.add("f3Store");
		expect(mockCache.getFamilyData(null, "table", "id", "f1Store")).andReturn(null);
		expect(mockCache.getFamilyData(null, "table", "id", "f2Store")).andReturn(null);
		expect(mockCache.getFamilyData(null, "table", "id", "f3Store")).andReturn(null);
		
		ColumnFamilyData v = new DefaultColumnFamilyData();
		Map<String, byte[]> value = new HashMap<String, byte[]>();
		value.put("Toto", new byte[10]);
		expect(mockStore.get(null, "table", "id", storedFamilies)).andReturn(v);
		for (String sf : storedFamilies) {
			v.put(sf, value);
			mockCache.insertFamilyData(null, "table", "id", sf, value);
		}
		replay();
		sut.get(null, "table", "id", storedFamilies);
		verify();
	}
	@Test
	public void test() throws CacheException{
		Set<String> cachedFamilies = new HashSet<String>();
		cachedFamilies.add("f1Cache");
		cachedFamilies.add("f2Cache");
		cachedFamilies.add("f3Cache");
		Set<String> storedFamilies = new HashSet<String>();
		storedFamilies.add("f1Store");
		storedFamilies.add("f2Store");
		storedFamilies.add("f3Store");
		Set<String> families=new HashSet<String>();
		families.addAll(cachedFamilies);
		families.addAll(storedFamilies);
		Map<String, byte[]> value = new HashMap<String, byte[]>();
		value.put("Toto", new byte[10]);
		
		expect(mockCache.getFamilyData(null, "table", "id", "f1Cache")).andReturn(value);
		expect(mockCache.getFamilyData(null, "table", "id", "f2Cache")).andReturn(value);
		expect(mockCache.getFamilyData(null, "table", "id", "f3Cache")).andReturn(value);
		expect(mockCache.getFamilyData(null, "table", "id", "f1Store")).andReturn(null);
		expect(mockCache.getFamilyData(null, "table", "id", "f2Store")).andReturn(null);
		expect(mockCache.getFamilyData(null, "table", "id", "f3Store")).andReturn(null);
		
		ColumnFamilyData v = new DefaultColumnFamilyData();
		for (String sf : storedFamilies) {
			v.put(sf, value);
			mockCache.insertFamilyData(null, "table", "id", sf, value);
		}
		expect(mockStore.get(null, "table", "id", storedFamilies)).andReturn(v);
		replay();
		sut.get(null, "table", "id", families);
		verify();

		
	}
	
	

}
