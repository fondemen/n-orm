package com.googlecode.n_orm;

import static org.junit.Assert.*;

import java.util.Set;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.Indexed;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.cf.MapColumnFamily;
import com.googlecode.n_orm.cf.SetColumnFamily;
import com.googlecode.n_orm.conversion.ConversionTools;


public class InheritanceTest {
	protected Ancestor a1, a2;
	protected Child1 c11, c12;
	protected Child2 c21, c22;
	
	protected Set<Ancestor> ancestors, allAncestors;
	protected Set<Child1> child1s;
	protected Set<Child2> child2s;
	
	public InheritanceTest() throws Exception {
		StoreTestLauncher.registerStorePropertiesForInnerClasses(getClass());
	}

	 @Persisting
	 public static class Ancestor {
		 @Key(order=1) public String key;

		public Ancestor(String key) {
			this.key = key;
		}
		 
	 }

	 @Persisting
	 public static class Child1 extends Ancestor {
		 @Key(order = 2) public String key2;

		public Child1(String key, String key2) {
			super(key);
			this.key2 = key2;
		}
	 }

	 @Persisting
	 public static class Child2 extends Ancestor {
		 @Key(order = 2) public String key2;

		public Child2(String key, String key2) {
			super(key);
			this.key2 = key2;
		}
	 }
	 
	 @Before
	 public void prepare() {
		 a1 = new Ancestor("a1");
		 a2 = new Ancestor("a2");
		 c11 = new Child1("a11", "c11");
		 c12 = new Child1("a12", "c12");
		 c21 = new Child2("a21", "c21");
		 c22 = new Child2("a22", "c22");
		 
		 ancestors = new TreeSet<Ancestor>(); ancestors.add(a1); ancestors.add(a2);
		 child1s = new TreeSet<Child1>(); child1s.add(c11); child1s.add(c12);
		 child2s = new TreeSet<Child2>(); child2s.add(c21); child2s.add(c22);
		 allAncestors = new TreeSet<Ancestor>(); allAncestors.addAll(ancestors); allAncestors.addAll(child1s); allAncestors.addAll(child2s);
	 }
	 
	 @Test
	 public void storeTable() throws DatabaseNotReachedException {
		for (Ancestor a : allAncestors) {
			a.store();
		}
		
		try {
			Set<Ancestor> ancestors = StorageManagement.findElements().ofClass(Ancestor.class).withAtMost(1000).elements().go();
			assertTrue(allAncestors.equals(ancestors));
			 
			Set<Child1> c1s = StorageManagement.findElements().ofClass(Child1.class).withAtMost(1000).elements().go();
			assertTrue(child1s.equals(c1s));
			 
			Set<Child2> c2s = StorageManagement.findElements().ofClass(Child2.class).withAtMost(1000).elements().go();
			assertTrue(child2s.equals(c2s));
			
		} finally {
			for (Ancestor a : allAncestors) {
				a.delete();
			}
		}
	 }
	 
	 @Test
	 public void arrayConversion() {
		 Ancestor[] ancestors = getAllAncestorsArray();
		 
		 String ancestorsAsString = ConversionTools.convertToString(ancestors);
		 assertArrayEquals(ancestors, ConversionTools.convertFromString(Ancestor[].class, ancestorsAsString));
		 
		 byte[] ancestorsAsBytes = ConversionTools.convert(ancestors);
		 assertArrayEquals(ancestors, ConversionTools.convert(Ancestor[].class, ancestorsAsBytes));
	 }

	private Ancestor[] getAllAncestorsArray() {
		return allAncestors.toArray(new Ancestor[allAncestors.size()]);
	}
	
	public static class AncestorKeyed {
		@Key public Ancestor key;
		public AncestorKeyed(Ancestor key) {
			this.key = key;
		}
	}
	 
	 @Persisting
	 public static class AncestorContainer {
		@Key public Ancestor[] aak;
		public Ancestor[] aap;
		@Indexed(field="key") public SetColumnFamily<Ancestor> ac = null;
		@Indexed(field="key") public SetColumnFamily<AncestorKeyed> ack = null;
		public MapColumnFamily<Ancestor, String> akm = null;
		public MapColumnFamily<String, Ancestor> avm = null;
		public AncestorContainer(Ancestor... aak) {
			this.aak = aak;
		}
	 }
	 
	 @Test
	 public void storeKeys() throws DatabaseNotReachedException {
		 AncestorContainer ac = new AncestorContainer(this.getAllAncestorsArray());
		 ac.store();
		 
		 try {
			 AncestorContainer acr = StorageManagement.findElements().ofClass(AncestorContainer.class).any();
		 
			 assertArrayEquals(this.getAllAncestorsArray(), acr.aak);
		 } finally {
			 ac.delete();
		 }
	 }
	 
	 @Test
	 public void storeProperty() throws DatabaseNotReachedException {
		 AncestorContainer ac = new AncestorContainer(a1);
		 ac.aap = this.getAllAncestorsArray();
		 ac.store();
		 
		 try {
			 AncestorContainer acr = StorageManagement.findElements().ofClass(AncestorContainer.class).any();
			 acr.activate();
		 
			 assertArrayEquals(this.getAllAncestorsArray(), acr.aap);
		 } finally {
			 ac.delete();
		 }
	 }
	 
	 @Test
	 public void storeCollectionValue() throws DatabaseNotReachedException {
		 AncestorContainer ac = new AncestorContainer(a1);
		 ac.ac.addAll(allAncestors);
		 ac.store();
		 
		 try {
			 AncestorContainer acr = StorageManagement.findElements().ofClass(AncestorContainer.class).any();
			 acr.activate("ac");
		 
			 assertTrue(allAncestors.containsAll(acr.ac));
			 assertTrue(acr.ac.containsAll(allAncestors));
		 } finally {
			 ac.delete();
		 }
	 }
	 
	 @Test
	 public void storeCollectionKey() throws DatabaseNotReachedException {
		 AncestorContainer ac = new AncestorContainer(a1);
		 for (Ancestor a : allAncestors) {
			ac.ack.add(new AncestorKeyed(a));
		}
		 ac.store();
		 
		 try {
			 AncestorContainer acr = StorageManagement.findElements().ofClass(AncestorContainer.class).any();
			 acr.activate("ack");
		 
			 assertTrue(ac.ack.containsAll(acr.ack));
			 assertTrue(acr.ack.containsAll(ac.ack));
		 } finally {
			 ac.delete();
		 }
	 }
	 
	 @Test
	 public void storeMapKey() throws DatabaseNotReachedException {
		 AncestorContainer ac = new AncestorContainer(a1);
		 int i = 0;
		 for (Ancestor a : allAncestors) {
			ac.akm.put(a, Integer.toString(i++));
		}
		 ac.store();
		 
		 try {
			 AncestorContainer acr = StorageManagement.findElements().ofClass(AncestorContainer.class).any();
			 acr.activate("akm");
		 
			 assertTrue(allAncestors.containsAll(acr.akm.keySet()));
			 assertTrue(acr.akm.keySet().containsAll(allAncestors));
		 } finally {
			 ac.delete();
		 }
	 }
	 
	 @Test
	 public void storeMapValue() throws DatabaseNotReachedException {
		 AncestorContainer ac = new AncestorContainer(a1);
		 int i = 0;
		 for (Ancestor a : allAncestors) {
			ac.avm.put(Integer.toString(i++), a);
		}
		 ac.store();
		 
		 try {
			 AncestorContainer acr = StorageManagement.findElements().ofClass(AncestorContainer.class).any();
			 acr.activate("avm");
		 
			 assertTrue(allAncestors.containsAll(acr.avm.values()));
			 assertTrue(acr.avm.values().containsAll(allAncestors));
		 } finally {
			 ac.delete();
		 }
	 }
}
