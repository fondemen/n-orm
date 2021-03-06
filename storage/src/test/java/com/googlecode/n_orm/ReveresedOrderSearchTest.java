package com.googlecode.n_orm;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.Date;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReveresedOrderSearchTest {
	
	public ReveresedOrderSearchTest() throws Exception {
		StoreTestLauncher.registerStorePropertiesForInnerClasses(getClass());
	}

	
	@Persisting(table="ReveresedOrderSearchTest")
	public static class FooStd {
		private static final long serialVersionUID = -4422829353876718397L;
		@Key public long bar;
	}
	
	@Persisting(table="ReveresedOrderSearchTest")
	public static class FooRevDate {
		private static final long serialVersionUID = -6255332472339355078L;
		@Key(reverted=true) public Date bar;
	}
	
	@Persisting(table="ReveresedOrderSearchTest")
	public static class FooRevString {
		private static final long serialVersionUID = 7020782950284603285L;
		@Key(reverted=true) public String bar;
	}
	
	@BeforeClass
	public static void truncate() {
		for (FooStd elt : StorageManagement.findElements().ofClass(FooStd.class).withAtMost(1000).elements().go()) {
			elt.delete();
		}
	}
	
	@After
	public void truncateInt() {
		truncate();
	}
	
	public void doTest(Class<? extends PersistingElement> clazz, Object k1, Object k2, Object k3)  {
		try {
			Field key = clazz.getDeclaredField("bar");
	
			PersistingElement f1 = clazz.newInstance(); key.set(f1, k1); f1.store();
			PersistingElement f2 = clazz.newInstance(); key.set(f2, k2); f2.store();
			PersistingElement f3 = clazz.newInstance(); key.set(f3, k3); f3.store();

			if (key.getAnnotation(Key.class).reverted()) {
				assertTrue(f2.compareTo(f1) <= 0);
				assertTrue(f3.compareTo(f2) <= 0);
				assertTrue(f3.compareTo(f1) <= 0);
			} else {
				assertTrue(f1.compareTo(f2) <= 0);
				assertTrue(f2.compareTo(f3) <= 0);
				assertTrue(f1.compareTo(f3) <= 0);
			}
				
			CloseableIterator<? extends PersistingElement> found = StorageManagement.findElements().ofClass(clazz).withKey("bar").greaterOrEqualsThan(k2).withAtMost(100).elements().iterate();
			try {
				assertTrue(found.hasNext());
				assertEquals(f2, found.next());
				if (key.getAnnotation(Key.class).reverted())
					assertEquals(f1, found.next());
				else
					assertEquals(f3, found.next());
				assertFalse(found.hasNext());
			} finally {
				found.close();
			}
		} catch (RuntimeException r) {
			throw r;
		} catch (Exception x) {
			throw new RuntimeException(x);
		}
	}
	
	@Test
	public void standardKeySearch() {
		doTest(FooStd.class, 10000l, 20000l, 30000l);
	}
	
//	@Test(expected=UnreversibleTypeException.class)
//	public void revertedKeySearchString() {
//		FooRevString f1 = new FooRevString(); f1.bar = "dsuiozkcjzio"; f1.store();
//	}
	
	@Test
	public void revertedKeySearchDate() {
		doTest(FooRevDate.class, new Date(10000), new Date(20000), new Date(30000));
	}

	
	@Persisting(table="ReveresedOrderSearchTest")
	public static class FooRevBool {
		private static final long serialVersionUID = -8161506031563020093L;
		@Key(reverted=true) public Boolean bar;
	}
	@Test
	public void revertedKeySearchBool() {
		doTest(FooRevBool.class, false, true, true);
	}

	
	@Persisting(table="ReveresedOrderSearchTest")
	public static class FooRevInt {
		private static final long serialVersionUID = -3277636999586287079L;
		@Key(reverted=true) public Integer bar;
	}
	@Test
	public void revertedKeySearchInt() {
		doTest(FooRevInt.class, 10000, 20000, 30000);
	}
	@Test
	public void revertedKeySearchIntNeg() {
		doTest(FooRevInt.class, -20000, -10000, 10000);
	}
	@Test
	public void revertedKeySearchIntNeg2() {
		doTest(FooRevInt.class, -20000, 10000, 20000);
	}

	
	@Persisting(table="ReveresedOrderSearchTest")
	public static class FooRevPInt {
		private static final long serialVersionUID = -7319454788789724765L;
		@Key(reverted=true) public int bar;
	}
	@Test
	public void revertedKeySearchPInt() {
		doTest(FooRevPInt.class, 10000, 20000, 30000);
	}
	@Test
	public void revertedKeySearchPIntNeg() {
		doTest(FooRevPInt.class, -20000, -10000, 10000);
	}
	@Test
	public void revertedKeySearchPIntNeg2() {
		doTest(FooRevPInt.class, -20000, 10000, 20000);
	}
	
	@Persisting(table="ReveresedOrderSearchTest")
	public static class FooRevByte {
		private static final long serialVersionUID = -3920675695988699567L;
		@Key(reverted=true) public byte bar;
	}
	@Test
	public void revertedKeySearchByte() {
		doTest(FooRevByte.class, (byte)10, (byte)20, (byte)30);
	}
	
	@Persisting(table="ReveresedOrderSearchTest")
	public static class FooRevShort {
		private static final long serialVersionUID = 8575670428412442926L;
		@Key(reverted=true) public short bar;
	}
	@Test
	public void revertedKeySearchShort() {
		doTest(FooRevShort.class, (short)10, (short)20, (short)30);
	}
}
