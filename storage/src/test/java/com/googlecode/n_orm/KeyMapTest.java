package com.googlecode.n_orm;

import static org.junit.Assert.*;

import org.junit.Test;

public class KeyMapTest {
	
	public static class Keyable {
		@Key(order=1) public String key1;
		@Key(order=2) public String key2;
		
		public Keyable() {
			fail("Should not use this constructor");
		}
		
		public Keyable(String val2, String val1, String val0) {
			fail("Should not use this constructor");
		}
		
		public Keyable(@KeyMap("key2") String val2, @KeyMap("key1") String val1) {
			this.key1 = val1;
			this.key2 = val2;
		}
		
		public Keyable(String val3, String val2, String val1, String val0) {
			fail("Should not use this constructor");
		}
	}

	@Test
	public final void nominal() {
		Keyable res = StorageManagement.getElementWithKeys(Keyable.class, "1", "2");
		assertEquals("1", res.key1);
		assertEquals("2", res.key2);
	}

	
	public static class MissingKeyable {
		@Key(order=1) public String key1;
		@Key(order=2) public String key2;
		
		public MissingKeyable(@KeyMap("key2") String val2) {
			this.key1 = val2;
		}
	}

	@Test(expected=Exception.class)
	public final void missingKey() {
		StorageManagement.getElementWithKeys(MissingKeyable.class, "1", "2");
	}

	
	public static class TooMuchKeyable {
		@Key(order=1) public String key1;
		@Key(order=2) public String key2;

		public TooMuchKeyable(@KeyMap("key1") String val1, @KeyMap("key2") String val2, @KeyMap("key2") String val3) {
			this.key1 = val1;
			this.key2 = val2;
		}
	}

	@Test(expected=Exception.class)
	public final void tooMuchKeys() {
		StorageManagement.getElementWithKeys(TooMuchKeyable.class, "1", "2");
	}

	
	public static class AdditionalKeyable {
		@Key(order=1) public String key1;
		@Key(order=2) public String key2;

		public AdditionalKeyable(@KeyMap("key1") String val1, @KeyMap("key2") String val2, String val3) {
			this.key1 = val1;
			this.key2 = val2;
		}
	}

	@Test(expected=Exception.class)
	public final void additionalArgument() {
		StorageManagement.getElementWithKeys(AdditionalKeyable.class, "1", "2");
	}

	
	public static class NotAKeyKeyable {
		@Key(order=1) public String key1;
		@Key(order=2) public String key2;

		public NotAKeyKeyable(@KeyMap("key1") String val1, @KeyMap("keyX") String val2) {
			this.key1 = val1;
			this.key2 = val2;
		}
	}

	@Test(expected=Exception.class)
	public final void notAKey() {
		StorageManagement.getElementWithKeys(NotAKeyKeyable.class, "1", "2");
	}
}
