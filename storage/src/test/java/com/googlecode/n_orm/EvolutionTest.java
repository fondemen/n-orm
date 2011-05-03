package com.googlecode.n_orm;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.Test;

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
		V0 v0 = new V0(); v0.key = key; v0.store();
		
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
	}
}
