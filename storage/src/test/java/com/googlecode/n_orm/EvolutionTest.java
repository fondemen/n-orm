package com.googlecode.n_orm;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.cf.SetColumnFamily;

public class EvolutionTest {
	
	public EvolutionTest() throws Exception {
		StoreTestLauncher.registerStorePropertiesForInnerClasses(getClass());
	}
	
	public static enum EnumTest {E1, E2, E3}

	@Persisting(table="evoltest")
	public static class V0 {
		@Key public String key;
	}

	@Persisting(table="evoltest")
	public static class V0NewProps {
		@Key public String key;
		
		public String sval;
		public Date tval;
		public int ival;
		public float dval;
		public EnumTest eval;
	}
	
	@Test
	public void newProps() {
		String key = "JIOJJ:?IKBYI:NIUBYBF";
		V0 v0 = new V0(); v0.key = key; v0.delete(); v0.store();
		
		V0NewProps v1 = new V0NewProps(), uninitialized = new V0NewProps(); v1.key = key;
		v1.sval = "huihjk";
		v1.tval = new Date();
		v1.ival = 457890;
		v1.dval = 7.786786e-12f;
		v1.eval = EnumTest.E2;
		v1.activate();
		
		assertEquals(uninitialized.sval, v1.sval);
		assertEquals(uninitialized.tval, v1.tval);
		assertEquals(uninitialized.ival, v1.ival);
		assertEquals(uninitialized.dval, v1.dval, Double.MIN_VALUE);
		assertEquals(uninitialized.eval, v1.eval);
		
		v1.store();
		assertTrue(v0.existsInStore());
	}

	@Persisting(table="evoltest")
	public static class V0NewCf {
		@Key public String key;
		
		public String aProp;
		
		public SetColumnFamily<String> cf = new SetColumnFamily<String>();
	}
	
	@Test
	public void newCf() {
		String key = "JIOJJ:?IKBYI:NIUBYBF";
		V0 v0 = new V0(); v0.key = key; v0.delete(); v0.store();
		
		KeyManagement.getInstance().cleanupKnownPersistingElements();
		
		V0NewCf v1 = new V0NewCf();v1.key = key;
		v1.activate("cf");
		
		assertTrue(v1.cf.isEmpty());

		v1.aProp = "toti";
		v1.cf.add("toto");
		
		v1.store();
		assertTrue(v0.existsInStore());
		
		KeyManagement.getInstance().cleanupKnownPersistingElements();
		
		v1 = new V0NewCf();v1.key = key;
		v1.activate("cf");
		assertEquals("toti", v1.aProp);
		assertEquals(1, v1.cf.size());
		assertEquals("toto", v1.cf.iterator().next());
	}
	
	@Persisting
	public static class AnotherClass {
		@Key public String key;
		public String prop;
		protected Set<String> cf = null;
	}
	
	@Test
	public void newCfSawnAfter() {
		String key = "JIOJJ:?IKBYI:NIUBYBF";
		AnotherClass sut = new AnotherClass();
		sut.key = key;
		sut.prop = "toto";
		sut.delete();
		sut.store();
		
		AnotherClass sut2 = new AnotherClass();
		sut2.key = key;
		sut2.activate("cf");
		
		assertEquals("toto", sut2.prop);
	}
}
