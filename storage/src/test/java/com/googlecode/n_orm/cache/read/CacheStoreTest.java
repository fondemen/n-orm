package com.googlecode.n_orm.cache.read;
import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PropertyManagement;
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
		public String property;
		public Map<String, Integer> family = null;
		
	}
	
	private CachedStore sut;
	private IMocksControl mocksControl = createControl();
	private Store mockStore=mocksControl.createMock(Store.class);
	private ICache mockCache=mocksControl.createMock(com.googlecode.n_orm.cache.read.ICache.class);
	
	@Before
	public void setUp() throws Exception {
		sut=new CachedStore(mockStore,mockCache);
		mocksControl.resetToDefault();
		
	}
	
	private void verify() {
		mocksControl.verify();
	}
	private void replay() {
		mocksControl.replay();
	}
	private void checkOrder() {
		mocksControl.checkOrder(true);
	}
	
	@Test
	public void getCache() {
		assertEquals(mockCache, sut.getCache());
	}
	
	@Test
	public void testGetWhenNotInCache() throws CacheException {
		this.checkOrder();
		final Map<String, byte[]> family=new HashMap<String,byte[]>();
		byte[] Dupond = ConversionTools.convert("Dupont");
		byte[] Jean = null;
		byte[] age= ConversionTools.convert("10");
		family.put("nom", Dupond);
		family.put("Prenom", Jean);
		family.put("age", age);
		expect(mockCache.getFamilyData(null, "table", "key", "family")).andReturn(null);
		expect(mockStore.get(null, "table", "key", "family")).andReturn(family);
		mockCache.insertFamilyData(null, "table", "key", "family", family);
		expect(mockCache.getFamilyData(null, "table", "key", "family")).andReturn(family).times(0, 1);
		replay();
		assertEquals(family, sut.get(null, "table", "key", "family"));
		verify();
		
	}
	
	@Test
	public void testGetWhenInCache() throws CacheException {
		this.checkOrder();
		final Map<String, byte[]> family=new HashMap<String,byte[]>();
		byte[] Dupond = ConversionTools.convert("Dupont");
		byte[] Jean = null;
		byte[] age= ConversionTools.convert("10");
		family.put("nom", Dupond);
		family.put("Prenom", Jean);
		family.put("age", age);
		expect(mockCache.getFamilyData(null, "table", "key", "family")).andReturn(family);
		replay();
		assertEquals(family, sut.get(null, "table", "key", "family"));
		verify();
		
	}
	
	@Test
	public void testExist() throws CacheException{
		this.checkOrder();
		Element e=new Element();
		e.key="tagada";
		e.property="tsoin tsoin";
		e.family .put("A", 12);
		e.family.put("B", 14);
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
		e.property="tsoin tsoin";
		e.family .put("A", 12);
		e.family.put("B", 14);
		MetaInformation meta=new MetaInformation().forElement(e);
		expect(mockCache.existsData(eq(meta), eq("table"), eq("key"), EasyMock.or(eq(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME), eq("family")))).andReturn(false);
		expect(mockStore.exists(meta, "table", "key", "family")).andReturn(false);
		replay();
		assertFalse(sut.exists(meta, "table", "key", "family"));
		verify();
	}
	
	@Test
	public void testDelete()throws CacheException{
		Element e=new Element();
		e.key="tagada";
		e.property="tsoin tsoin";
		e.family.put("A", 12);
		e.family.put("B", 14);
		MetaInformation meta=new MetaInformation();
		mockCache.delete(meta, "table", "key");
		mockStore.delete(meta, "table", "key");
		replay();
		sut.delete(meta, "table", "key");
		verify();
	}
	
	@Test
	public void testExists() throws CacheException{
		this.checkOrder();
		Element e=new Element();
		e.key="tagada";
		e.property="tsoin tsoin";
		e.family.put("A", 12);
		e.family.put("B", 14);
		MetaInformation meta=new MetaInformation().forElement(e);
		expect(mockCache.existsData(eq(meta), eq("table"), eq("row"), EasyMock.or(eq(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME), eq("family")))).andReturn(true);
		replay();
		assertTrue(sut.exists(meta, "table", "row"));
		verify();
	}
	
	@Test
	public void testExistsButNotInCache() throws CacheException{
		Element e=new Element();
		e.key="tagada";
		e.property="tsoin tsoin";
		e.family.put("A", 12);
		e.family.put("B", 14);
		MetaInformation meta=new MetaInformation().forElement(e);
		expect(mockCache.existsData(meta, "table", "row", PropertyManagement.PROPERTY_COLUMNFAMILY_NAME)).andReturn(false);
		expect(mockCache.existsData(meta, "table", "row", "family")).andReturn(false);
		expect(mockStore.exists(meta, "table", "row")).andReturn(true);
		replay();
		assertTrue(sut.exists(meta, "table", "row"));
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
	public void testGetMixedCachedAndNotCached() throws CacheException{
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
		Map<String, byte[]> valueCached = new HashMap<String, byte[]>();
		valueCached.put("Toto", new byte[] {(byte)46});
		Map<String, byte[]> valueStored = new HashMap<String, byte[]>();
		valueStored.put("Toto", new byte[] {(byte)-9});

		ColumnFamilyData storedValues = new DefaultColumnFamilyData();
		ColumnFamilyData returnedValues = new DefaultColumnFamilyData();
		for (String sf : storedFamilies) {
			storedValues.put(sf, valueStored);
			returnedValues.put(sf, valueStored);
			expect(mockCache.getFamilyData(null, "table", "id", sf)).andReturn(null);
			mockCache.insertFamilyData(null, "table", "id", sf, valueStored);
		}
		for (String sf : cachedFamilies) {
			returnedValues.put(sf, valueCached);
			expect(mockCache.getFamilyData(null, "table", "id", sf)).andReturn(valueCached);
		}
		expect(mockStore.get(null, "table", "id", storedFamilies)).andReturn(storedValues);
		replay();
		assertEquals(returnedValues, sut.get(null, "table", "id", families));
		verify();

		
	}
	
	@Test
	public void testGetCached() throws CacheException{
		Set<String> cachedFamilies = new HashSet<String>();
		cachedFamilies.add("f1Cache");
		cachedFamilies.add("f2Cache");
		cachedFamilies.add("f3Cache");
		Set<String> families=new HashSet<String>();
		families.addAll(cachedFamilies);
		Map<String, byte[]> valueCached = new HashMap<String, byte[]>();
		valueCached.put("Toto", new byte[] {(byte)46});
		Map<String, byte[]> valueStored = new HashMap<String, byte[]>();
		valueStored.put("Toto", new byte[] {(byte)-9});

		ColumnFamilyData returnedValues = new DefaultColumnFamilyData();
		for (String sf : cachedFamilies) {
			returnedValues.put(sf, valueCached);
			expect(mockCache.getFamilyData(null, "table", "id", sf)).andReturn(valueCached);
		}
		replay();
		assertEquals(returnedValues, sut.get(null, "table", "id", families));
		verify();

		
	}
	
	@Test
	public void testGetNotCached() throws CacheException{
		Set<String> storedFamilies = new HashSet<String>();
		storedFamilies.add("f1Store");
		storedFamilies.add("f2Store");
		storedFamilies.add("f3Store");
		Set<String> families=new HashSet<String>();
		families.addAll(storedFamilies);
		Map<String, byte[]> valueCached = new HashMap<String, byte[]>();
		valueCached.put("Toto", new byte[] {(byte)46});
		Map<String, byte[]> valueStored = new HashMap<String, byte[]>();
		valueStored.put("Toto", new byte[] {(byte)-9});

		ColumnFamilyData storedValues = new DefaultColumnFamilyData();
		ColumnFamilyData returnedValues = new DefaultColumnFamilyData();
		for (String sf : storedFamilies) {
			storedValues.put(sf, valueStored);
			returnedValues.put(sf, valueStored);
			expect(mockCache.getFamilyData(null, "table", "id", sf)).andReturn(null);
			mockCache.insertFamilyData(null, "table", "id", sf, valueStored);
		}
		expect(mockStore.get(null, "table", "id", storedFamilies)).andReturn(storedValues);
		replay();
		assertEquals(returnedValues, sut.get(null, "table", "id", families));
		verify();

		
	}
	
	

}
