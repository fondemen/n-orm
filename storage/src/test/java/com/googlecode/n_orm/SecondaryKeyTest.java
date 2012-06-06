package com.googlecode.n_orm;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

public class SecondaryKeyTest {
	@Persisting
	@SecondaryKeys({"sk1"})
	public static class Element {
		@Key public String key;
		@SecondaryKey("sk1") public String sk11;
		@SecondaryKey("sk1#2") public String sk12;
	}
	
	@Test
	public void skIdentifierCalculation() {
		Element elt = new Element();
		elt.key = "key";
		elt.sk11 = "sk11";
		elt.sk12 = "sk12";
		
		assertEquals("sk11" + KeyManagement.KEY_SEPARATOR + "sk12" + KeyManagement.KEY_END_SEPARATOR, elt.getIdentifierForSecondaryKey("sk1"));
	}

}
