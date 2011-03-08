package com.mt.storage.conversion;

import static org.junit.Assert.*;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;

import org.junit.Test;

import com.mt.storage.Key;
import com.mt.storage.Persisting;

public class ConversionTest {

	public <NUMBER, OBJECT> void convTest(Class<NUMBER> numberClass, Class<OBJECT> objectClass, OBJECT [] values, boolean checkString) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		String [] res = new String [values.length];
		boolean testNumber = numberClass != null;
		for (int p = 0; p < values.length; p++) {
			OBJECT io = values[p];

			byte [] bytes, nrBytes = null;
			
			if (testNumber) {
				@SuppressWarnings("unchecked")
				NUMBER i = (NUMBER) objectClass.getMethod(numberClass.getName() + "Value").invoke(io);
				nrBytes = ConversionTools.convert(i, numberClass);
				assertEquals(i, (NUMBER)ConversionTools.convert(numberClass, nrBytes));
				assertEquals(io, ConversionTools.convert(objectClass, nrBytes));
			}
			
			bytes = ConversionTools.convert(io, objectClass);
			if (testNumber) {
				assertNotNull(nrBytes);
				assertArrayEquals(nrBytes, bytes);
			}
			assertEquals(io, ConversionTools.convert(objectClass, bytes));
			
			if (checkString) {
				res[p] = ConversionTools.convertToString(io, objectClass);
				assertEquals(io, ConversionTools.convertFromString(objectClass, res[p]));
				if (testNumber) {
					@SuppressWarnings("unchecked")
					NUMBER i = (NUMBER) objectClass.getMethod(numberClass.getName() + "Value").invoke(io);
					assertEquals(res[p], ConversionTools.convertToString(i, numberClass));
					assertEquals(i, (NUMBER)ConversionTools.convertFromString(numberClass, res[p]));
				}
			} else {
				try {
					ConversionTools.convertToString(io, objectClass);
					fail("Was expecting exception");
				} catch (IllegalArgumentException x) {}
			}
			
		}
		
		if (checkString) {
			for(int p = 1; p < res.length; ++p) {
				assertTrue(res[p-1].compareTo(res[p]) <= 0);
			}
		}
		
		byte [] arrayBytes = ConversionTools.convert(values, values.getClass());
		@SuppressWarnings("unchecked")
		Class<? extends Object[]> arrayClass = (Class<? extends Object[]>) Array.newInstance(objectClass, 0).getClass();
		assertArrayEquals(values, ConversionTools.convert(arrayClass, arrayBytes));
		
		if (checkString) {
			String arrayString = ConversionTools.convertToString(values, values.getClass());
			assertArrayEquals(values, ConversionTools.convertFromString(arrayClass, arrayString));
		}
	}
	
	@Test
	public void intConv() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this.convTest(int.class, Integer.class, new Integer [] {Integer.MIN_VALUE, -1235, -1, 0, 1, 48, Integer.MAX_VALUE}, true);
	}
	
	@Test
	public void byteConv() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this.convTest(byte.class, Byte.class, new Byte [] {Byte.MIN_VALUE, -59, -1, 0, 1, 20, Byte.MAX_VALUE}, true);
	}
	
	@Test
	public void shortConv() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this.convTest(short.class, Short.class, new Short [] {Short.MIN_VALUE, -594, -1, 0, 1, 201, Short.MAX_VALUE}, true);
	}
	
	@Test
	public void longConv() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this.convTest(long.class, Long.class, new Long [] {Long.MIN_VALUE, -5945l, -1l, 0l, 1l, 201l, Long.MAX_VALUE}, true);
	}
	
	@Test
	public void floatConv() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this.convTest(float.class, Float.class, new Float [] {Float.MIN_VALUE, -5945.135848f, -1f, -Float.MIN_NORMAL, 0f,-Float.MIN_NORMAL, 1f, 1.45645e3f, Float.MAX_VALUE}, false);
	}
	
	@Test
	public void doubleConv() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this.convTest(double.class, Double.class, new Double [] {Double.MIN_VALUE, -55.135848e123, -1d, -Double.MIN_NORMAL, 0d,-Double.MIN_NORMAL, 1d, 1.455464548654645e34d, Double.MAX_VALUE}, false);
	}
	
	@Test
	public void boolConv() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this.convTest(boolean.class, Boolean.class, new Boolean [] {false, true}, true);
	}
	
	@Test
	public void charConv() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this.convTest(char.class, Character.class, new Character [] {Character.MIN_VALUE, 1, 'B', 'Z', 'a', 'k', 512, '\uFFFE'}, true);
		//\uFFFF is reserved as the array value separator
	}
	
	@Test
	public void stringConv() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this.convTest(null, String.class, new String [] {"123", "123456", "22"}, true);
	}
	
	@Test
	public void dateConv() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this.convTest(null, Date.class, new Date[] {new Date(0), new Date(123456), new Date(Long.MAX_VALUE)}, true);
	}
	
	public static enum TestEnum {v1,v2,v3,v4};
	@Test
	public void enumConv() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this.convTest(null, ConversionTest.TestEnum.class, new ConversionTest.TestEnum [] {ConversionTest.TestEnum.v1, ConversionTest.TestEnum.v2, ConversionTest.TestEnum.v3, ConversionTest.TestEnum.v4}, true);
	}
	
	public static class Keyable1Key {
		@Key public  String key;

		public Keyable1Key(String key) {
			super();
			this.key = key;
		}

		@Override
		public boolean equals(Object obj) {
			return obj != null && (obj instanceof Keyable1Key) && this.key.equals(((Keyable1Key)obj).key);
		}
	}
	@Test
	public void oneKeyConv() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this.convTest(null, Keyable1Key.class, new Keyable1Key [] {new Keyable1Key("123"), new Keyable1Key("123456"), new Keyable1Key("22")}, true);
	}
	
	public static class Keyable2Keys {
		@Key public  int key1;
		@Key(order=2) public  String key2;

		public Keyable2Keys(int key1, String key2) {
			super();
			this.key1 = key1;
			this.key2 = key2;
		}

		@Override
		public boolean equals(Object obj) {
			return obj != null && (obj instanceof Keyable2Keys) && this.key1 == ((Keyable2Keys)obj).key1 && this.key2.equals(((Keyable2Keys)obj).key2);
		}
	}
	@Test
	public void twoKeyConv() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this.convTest(null, Keyable2Keys.class, new Keyable2Keys [] {new Keyable2Keys(1, "123"), new Keyable2Keys(1, "123456"), new Keyable2Keys(2, "123"), new Keyable2Keys(10, "123"), new Keyable2Keys(20, "22")}, true);
	}
	
	@Persisting
	public static class Persistable {
		@Key public  String key;
		public String value;
		
		public Persistable(String key) {
			this.key = key;
		}
		
		public Persistable(String key, String value) {
			this(key);
			this.value = value;
		}

		@Override
		public boolean equals(Object obj) {
			return obj != null && (obj instanceof Persistable) && this.key.equals(((Persistable)obj).key);
		}
	}
	@Test
	public void persistingConv() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this.convTest(null, Persistable.class, new Persistable [] {new Persistable("1", "123"), new Persistable("10", "123"), new Persistable("2", "123"), new Persistable("20", "22")}, true);
	}
	
	@Test
	public void emptyArray() {
		int[] array = new int[0];
		byte [] rep = ConversionTools.convert(array, int[].class);
		assertEquals(0, ConversionTools.convert(int[].class, rep).length);
	}
	
	@Test
	public void notEmptyArray() {
		int[] array = {1, 2, 9, -12, 0};
		byte [] rep = ConversionTools.convert(array, int[].class);
		assertArrayEquals(array, ConversionTools.convert(int[].class, rep));
	}
	
	@Test
	public void nullArray() {
		int[] array = null;
		byte [] rep = ConversionTools.convert(array, int[].class);
		assertNull(ConversionTools.convert(int[].class, rep));
	}
	
	@Test
	public void bytesArray() {
		byte[] array = new byte[] {1,2,-3};
	}
	
	@Test
	public void multidimensionalIntArray() {
		int[][] array = new int[][] {{1,2,-3}, {}, {4,5}};
		byte[] res = ConversionTools.convert(array, array.getClass());
		assertArrayEquals(array, ConversionTools.convert(array.getClass(), res));
	}
	
	@Test(expected=Exception.class)
	public void multidimensionalIntArrayStringified() {
		int[][] array = new int[][] {{1,2,-3}, {}, {4,5}};
		ConversionTools.convertToString(array, array.getClass());
	}
	
	public static class Inner {
		@Key(order = 1) public  String k1;
		@Key(order = 2) public  String k2;
		public Inner(String k1, String k2) {
			this.k1 = k1;
			this.k2 = k2;
		}
		@Override public boolean equals(Object rhs) {
			return (rhs instanceof Inner) && this.k1.equals(((Inner)rhs).k1) && this.k2.equals(((Inner)rhs).k2);
		}
	}
	public static class Outer {
		@Key(order=1) public  Inner[] k1;
		@Key(order=2) public  Inner[] k2;
		public Outer(Inner[] k1, Inner[] k2) {
			this.k1 = k1;
			this.k2 = k2;
		}
		
	}
	@Test public void arrayKeys() {
		Inner i11 = new Inner("i111", "i112");
		Inner i12 = new Inner("i121", "i122");
		Inner i13 = new Inner("i131", "i132");
		Inner i21 = new Inner("i211", "i212");
		Inner i22 = new Inner("i221", "i222");
		Inner[] i1 = new Inner[]{i11, i12, i13};
		Inner[] i2 = new Inner[]{i21, i22};
		Outer sut = new Outer(i1, i2);
		String sutAsString = ConversionTools.convertToString(sut, Outer.class);
		Outer sutBack = ConversionTools.convertFromString(Outer.class, sutAsString);
		assertArrayEquals(i1, sutBack.k1);
		assertArrayEquals(i2, sutBack.k2);
	}
	
	public static class Inner2 {
		@Key(order = 1) public  String[] k;
		public Inner2(String... k) {
			this.k = k;
		}
		@Override public boolean equals(Object rhs) {
			if (!(rhs instanceof Inner2))
				return false;
			Inner2 ri = (Inner2)rhs;
			if (this.k.length != ri.k.length)
				return false;
			for (int i = 0; i < this.k.length; i++) {
				if (!this.k[i].equals(ri.k[i]))
					return false;
			}
			return  true;
		}
	}
	public static class Outer2 {
		@Key(order=1) public  Inner2[] k;
		public Outer2(Inner2... k) {
			this.k = k;
		}
		
	}
	@Test public void multidimensionalArrayKeys() {
		Inner2 i11 = new Inner2("i111", "i112", "i113");
		Inner2 i12 = new Inner2("i121", "i122");
		Inner2 i13 = new Inner2("i131", "i132");
		Outer2 sut = new Outer2(i11, i12, i13);
		String sutAsString = ConversionTools.convertToString(sut);
		Outer2 sutBack = ConversionTools.convertFromString(Outer2.class, sutAsString);
		assertArrayEquals(new Inner2[] {i11, i12, i13}, sutBack.k);
	}
	
	public static class Outer3 {
		@Key(order=1) public  String f;
		@Key(order=2) public  Inner2[] k;
		public Outer3(String f, Inner2... k) {
			this.f = f;
			this.k = k;
		}
		
	}
	@Test public void multidimensionalArrayKeys2() {
		Inner2 i11 = new Inner2("i111", "i112", "i113");
		Inner2 i12 = new Inner2("i121", "i122");
		Inner2 i13 = new Inner2("i131", "i132");
		Outer3 sut = new Outer3("sep", i11, i12, i13);
		String sutAsString = ConversionTools.convertToString(sut);
		Outer3 sutBack = ConversionTools.convertFromString(Outer3.class, sutAsString);
		assertArrayEquals(new Inner2[] {i11, i12, i13}, sutBack.k);
	}
}
