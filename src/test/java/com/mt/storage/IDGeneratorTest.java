package com.mt.storage;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.mt.storage.conversion.ConversionTools;


public class IDGeneratorTest {
	private static enum ExampleEnum {e1, e2};
	@Persisting private static class Nominal {
		@Key(order=3) public final String k1;
		@Key(order=2) public final int k2;
		@Key(order=1) public final ExampleEnum k3;
		public String anotherProperty;
		public Nominal(String k1, int k2, ExampleEnum k3) {
			super();
			this.k1 = k1;
			this.k2 = k2;
			this.k3 = k3;
		}
		
	}
	
	@Test public void nominal3Keys() {
		Nominal po = new Nominal("k1val", -3, ExampleEnum.e2);
		Assert.assertEquals("e2:" + ConversionTools.convertToString(-3, int.class) + ":k1val", po.getIdentifier());
	}
	
	private static class InheritingNominal extends Nominal {
		public InheritingNominal(String k1, int k2, ExampleEnum k3) {
			super(k1, k2, k3);
		}
		
	}
	
	@Test public void simpleInheritance() {
		InheritingNominal po = new InheritingNominal("k1val", -3, ExampleEnum.e2);
		Assert.assertEquals("e2:" + ConversionTools.convertToString(-3, int.class) + ":k1val", po.getIdentifier());
	}
	
	private static class InheritingNominalWithOneMoreKey extends Nominal {
		@Key(order=4) public final String k4;
		public InheritingNominalWithOneMoreKey(String k1, int k2, ExampleEnum k3, String k4) {
			super(k1, k2, k3);
			this.k4 = k4;
		}
		
	}
	
	@Test public void inheritanceAdditionalKey() {
		InheritingNominalWithOneMoreKey po = new InheritingNominalWithOneMoreKey("k1val", -3, ExampleEnum.e2, "k4val");
		Assert.assertEquals("e2:" + ConversionTools.convertToString(-3, int.class) + ":k1val:k4val", po.getIdentifier());
	}
	
	private static class InheritingNominalWithMissingKey extends Nominal {
		@Key(order=5) public final String k4;
		public InheritingNominalWithMissingKey(String k1, int k2, ExampleEnum k3, String k4) {
			super(k1, k2, k3);
			this.k4 = k4;
		}
		
	}
	
	@Test(expected=IllegalArgumentException.class) public void lackInKeyOrder() {
		new InheritingNominalWithMissingKey("k1val", -3, ExampleEnum.e2, "k4val");
	}
	
	private static class InheritingNominalWithOverloadedKey extends Nominal {
		@Key(order=2) public final String k4;
		public InheritingNominalWithOverloadedKey(String k1, int k2, ExampleEnum k3, String k4) {
			super(k1, k2, k3);
			this.k4 = k4;
		}
		
	}
	
	@Test(expected=IllegalArgumentException.class) public void ubiquitousKeyOrder() {
		new InheritingNominalWithOverloadedKey("k1val", -3, ExampleEnum.e2, "k4val");
	}
	
	@Persisting	private static class KeWith0Index extends Nominal {
		@Key(order=0) public final String k4;
		public KeWith0Index(String k1, int k2, ExampleEnum k3, String k4) {
			super(k1, k2, k3);
			this.k4 = k4;
		}
		
	}
	
	@Test(expected=IllegalArgumentException.class) public void zeroKeyOrder() {
		new KeWith0Index("k1val", -3, ExampleEnum.e2, "k4val");
	}
	
	@Persisting	private static class PersitingWithObjectKey {
		@Key(order=1) public final Object k4;
		public PersitingWithObjectKey(Object k4) {
			this.k4 = k4;
		}
		
	}
	
	@Test(expected=IllegalStateException.class) public void illegalKeyType() {
		new PersitingWithObjectKey("k1val");
	}
	
	@Persisting	private static class NoKey {
	}
	
	@Test(expected=IllegalStateException.class) public void noKey() {
		new NoKey();
	}
	
	@Test(expected=IllegalStateException.class) public void notPersistingWhileImplementingPersistinElement() {
		new PersistingElement() {
			@SuppressWarnings("unused")
			@Key(order=1) public final int key = 1;
			
			@SuppressWarnings("unused")
			public List<Field> getKeys() {
				return null;
			}
			
			@SuppressWarnings("unused")
			public String getIdentifier() {
				return null;
			}
		};
	}
	
	@Persisting	private static class PersitingOwingPersisting {
		@Key(order=1) public final Nominal el;
		public PersitingOwingPersisting(String k1, int k2, ExampleEnum k3) {
			this.el = new Nominal(k1, k2, k3);
		}
	}
	
	@Test public void persistinOwningPersisting() {
		PersitingOwingPersisting po = new PersitingOwingPersisting("k1val", -3, ExampleEnum.e2);
		Assert.assertEquals("e2:" + ConversionTools.convertToString(-3, int.class) + ":k1val", po.getIdentifier());
	}
	
	@Persisting private static class PersistingClassWithNonFinalKey {
		@Key(order=1) public boolean key = false;
	}
	@Test(expected=IllegalStateException.class) public void persistingClassWithNonFinalKey() {
		new PersistingClassWithNonFinalKey();
	}

	@Persisting
	public static class AllTypesPersister {
		@Key public final Date k1;
		@Key(order = 2) public final String k2;
		@Key(order = 3) public final boolean k3;
		@Key(order = 4) public final int k4;
		@Key(order = 5) public final byte k5;
		@Key(order = 6) public final short k6;
		@Key(order = 7) public final long k7;
		@Key(order = 8) public final char k10;
		@Key(order = 9) public final Integer k11;

		public AllTypesPersister(Date k1, String k2, boolean k3, int k4,
				byte k5, short k6, long k7, char k10,
				Integer k11) {
			this.k1 = k1;
			this.k2 = k2;
			this.k3 = k3;
			this.k4 = k4;
			this.k5 = k5;
			this.k6 = k6;
			this.k7 = k7;
			this.k10 = k10;
			this.k11 = k11;
		}
	}

	@Test
	public void getFromId1() throws DatabaseNotReachedException {
		Calendar cal = Calendar.getInstance();
		cal.set(1917, 12, 27);
		String id = 
				ConversionTools.convertToString(cal.getTime(), Date.class) + ":" +
				"a string with an �:" +
				"1:" +
				ConversionTools.convertToString(-3678, int.class) + ":" +
				ConversionTools.convertToString((byte)126, byte.class) + ":" +
				ConversionTools.convertToString((short)356, short.class) + ":" +
				ConversionTools.convertToString(326783627l, long.class) + ":" +
				"\ua123:" +
				ConversionTools.convertToString(467, int.class);
		AllTypesPersister sut = ConversionTools
				.convertFromString(
						AllTypesPersister.class,
						id);
		assertEquals(cal.getTime(), sut.k1);
		assertEquals("a string with an �", sut.k2);
		assertEquals(true, sut.k3);
		assertEquals(-3678, sut.k4);
		assertEquals((byte)126, sut.k5);
		assertEquals((short)356, sut.k6);
		assertEquals(326783627l, sut.k7);
		assertEquals('\ua123', sut.k10);
		assertEquals(new Integer(467), sut.k11);
		
		sut.store();
		AllTypesPersister sut2 = ConversionTools.convertFromString(AllTypesPersister.class, ConversionTools.convertToString(sut, AllTypesPersister.class));
		assertEquals(sut.k1, sut2.k1);
		assertEquals(sut.k2, sut2.k2);
		assertEquals(sut.k3, sut2.k3);
		assertEquals(sut.k4, sut2.k4);
		assertEquals(sut.k5, sut2.k5);
		assertEquals(sut.k6, sut2.k6);
		assertEquals(sut.k7, sut2.k7);
		assertEquals(sut.k10, sut2.k10);
		assertEquals(sut.k11, sut2.k11);
		
		sut2 = ConversionTools.convert(AllTypesPersister.class, ConversionTools.convert(sut, AllTypesPersister.class));
		assertEquals(sut.k1, sut2.k1);
		assertEquals(sut.k2, sut2.k2);
		assertEquals(sut.k3, sut2.k3);
		assertEquals(sut.k4, sut2.k4);
		assertEquals(sut.k5, sut2.k5);
		assertEquals(sut.k6, sut2.k6);
		assertEquals(sut.k7, sut2.k7);
		assertEquals(sut.k10, sut2.k10);
		assertEquals(sut.k11, sut2.k11);
	}
}
