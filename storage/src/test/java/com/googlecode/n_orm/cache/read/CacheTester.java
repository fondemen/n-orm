package com.googlecode.n_orm.cache.read;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.xml.crypto.Data;

import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.cache.write.FixedThreadPool;
import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.storeapi.MetaInformation;


public abstract class CacheTester {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
	}
	
	@Test
	public void parallelGetFamilyData() throws InterruptedException, ExecutionException, CacheException{
		final ICache gc=createCache();
		final int parallelGet=10;
		Map<String, byte[]> familyData=new HashMap<String, byte[]>();
		byte[] Dupond = ConversionTools.convert("Dupont");
		byte[] Jean = null;
		byte[] age= ConversionTools.convert("10");
		familyData.put("nom", Dupond);
		familyData.put("Prenom", Jean);
		familyData.put("age", age);
		gc.insertFamilyData(null, "Person", "290", "props", familyData);
		
		Callable<Map<String, byte[]> > r=new Callable<Map<String, byte[]> >() {

			@Override
			public Map<String, byte[]> call() throws Exception {
				return gc.getFamilyData(null, "Person", "290", "props");
			}
		};
	
		Collection<Future<Map<String, byte[]>>> results=new LinkedList<Future<Map<String,byte[]>>>();
		ExecutorService es= new FixedThreadPool(parallelGet);
		for (int i = 0; i<parallelGet; i++){
			results.add(es.submit(r));
		}
		es.shutdown();

		for(Future<Map<String, byte[]>> f : results){
			assertTrue(comparatorValues(familyData, f.get()));
		}
		assertEquals(1,gc.size());
	}

	public abstract ICache createCache();

	@Test
	public void testBytesEquals() {
		final Map<String, byte[]> familyData=new HashMap<String, byte[]>();
		familyData.put("azerty", new byte[] {1,2});
		final Map<String, byte[]> familyData2=new HashMap<String, byte[]>();
		familyData2.put("azerty", new byte[] {1,2});
		assertTrue(comparatorValues(familyData,familyData2));
	}
	
	public static boolean comparatorValues(Map<String, byte[]> map1, Map<String,byte[]> map2){
		if(map1.size() != map2.size()){
			return false;
		}
        for(Entry<String, byte[]> k: map1.entrySet())
		{
        	if(Arrays.equals(k.getValue(), map2.get(k.getKey()))){
					return true;}
			else
				return false;
		}
			return false;
	}
	
	@Test
	public void parallelFamilyData() throws CacheException, InterruptedException, ExecutionException{
		
		final ICache gc=createCache();
		final int parallelThread = 100;
		
		final Map<String, byte[]> familyData=new HashMap<String, byte[]>();
		final Map<String, byte[]> animalData=new HashMap<String, byte[]>();
		
		familyData.put("FirstName", ConversionTools.convert(10));
		familyData.put("SecondName", ConversionTools.convert(20));
		gc.insertFamilyData(null, "Person", "300", "Props", familyData);
		
		
		animalData.put("Age", ConversionTools.convert("20"));
		animalData.put("Name", ConversionTools.convert("Lion"));
		gc.insertFamilyData(null, "Animal", "235", "Carnivore", animalData);
		
		
		Callable<Tuple<String, Map<String, byte[]>>> r = new Callable<Tuple<String, Map<String, byte[]>>>() {
			@Override
			public Tuple<String, Map<String, byte[]>> call() throws Exception {
				return new Tuple<String, Map<String, byte[]>>("Person", gc.getFamilyData(null, "Person", "300", "Props"));
			}
		};
		
		Callable<Tuple<String, Map<String, byte[]>>> rr = new Callable<Tuple<String, Map<String, byte[]>>>() {
			@Override
			public Tuple<String, Map<String, byte[]>> call() throws Exception {
				return new Tuple<String, Map<String, byte[]>>("Animal", gc.getFamilyData(null, "Animal", "235", "Carnivore"));
					
			}
		};
		Collection<Future<Tuple<String, Map<String, byte[]>>>> results=new LinkedList<Future<Tuple<String, Map<String, byte[]>>>>();
		ExecutorService es= new FixedThreadPool(parallelThread);
		for (int i = 0; i<parallelThread; i++){
			if(i %2==0){
			   results.add(es.submit(r));
			   }
			else{
				results.add(es.submit(rr));
			}
		}
		es.shutdown();
		
		for(Future<Tuple<String, Map<String, byte[]>>> f : results)
		{
			if(f.get().getX().equals("Person")){
				assertTrue(comparatorValues(familyData, f.get().getY()));
				}
			else{assertTrue(comparatorValues(animalData, f.get().getY())); /*iciiiiiiiiii*/
			}
		}
	}
	
	@Test
	public void testGetSize() throws Exception{
		ICache gc=createCache();
		long size=0;
		assertEquals(size, gc.size());
	}
	@Test
	public void testGetMaximunSize() throws CacheException{
		ICache gc=createCache();
		gc.setMaximunSize(200);
		assertEquals(200, gc.getMaximunSize());
	}
	@Test
	public void testGetTTL() throws CacheException {
		ICache gc=createCache();
		gc.setTTL(20);
		assertEquals(20, gc.getTTL());
	}
	@Test
	public void testSetTTL() throws CacheException {
		ICache gc=createCache();
		gc.setTTL(20);
		assertEquals(20, gc.getTTL());
		assertNotSame(10, gc.getTTL());
	}
	@Test
	public void testSetMaximunSize() throws CacheException{
		ICache gc=createCache();
		gc.setMaximunSize(200);
		assertEquals(200, gc.getMaximunSize());
	}
	@Test
	public void testReset() throws CacheException{
		ICache gc=createCache();
		Map<String, byte[]> familyData=new HashMap<String, byte[]>();
		byte[] Dupond = ConversionTools.convert("Dupont");
		byte[] Jean = null;
		byte[] age= ConversionTools.convert("10");
		familyData.put("nom", Dupond);
		familyData.put("Prenom", Jean);
		familyData.put("age", age);
		gc.insertFamilyData(null, "Person", "290", "props", familyData);
		gc.reset();
		long size=gc.size();
		assertEquals(0,size);
	}
	@Test
	public void testExistsData() throws CacheException{
		ICache gc=createCache();
		ICache gc2=createCache();
		Map<String, byte[]> familyData=new HashMap<String, byte[]>();
		byte[] Dupond = ConversionTools.convert("Dupont");
		byte[] Jean = null;
		byte[] age= ConversionTools.convert("10");
		familyData.put("nom", Dupond);
		familyData.put("Prenom", Jean);
		familyData.put("age", age);
		gc.insertFamilyData(null, "Person", "290", "props", familyData);
		boolean t=gc.existsData(null, "Person", "290", "props");
		assertTrue(t);
		boolean tt=gc2.existsData(null, "table", "key", "family");
		assertFalse(tt);
		
	}
	@Test
	public void testDelete() throws CacheException{
		Element e=new Element();
		String key="tagada";
		e.key=key;
		e.familyName="props";
		e.familyData.put("Age", 12);
		e.familyData.put("name", 23);
		MetaInformation meta=new MetaInformation();
		meta.forElement(e);
		ICache gc=createCache();
		Map<String, byte[]> data=new HashMap<String,byte[]>();
		data.put("", new byte[10]);
		data.put("", new byte[20]);
		data.put("", new byte[30]);
		
		for (String cfn : e.getColumnFamilyNames()) {
			ColumnFamily<?> cf = e.getColumnFamily(cfn);
			System.out.println(cf.getName());
		}
		gc.insertFamilyData(meta, "table", key, "familyData", data);
		gc.delete(meta, "table", key);
		assertEquals(null,gc.getFamilyData(meta, "table", key, "familyData"));
	}
}

