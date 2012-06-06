package com.googlecode.n_orm;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class SecondaryKeyComputationTest {
	@Persisting
	@SecondaryKeys("sk1")
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

		Set<SecondaryKeyDeclaration> sks = SecondaryKeyManagement.getInstance().getSecondaryKeyDeclarations(Element.class.asSubclass(PersistingElement.class));
		assertEquals(1, sks.size());
		assertEquals("sk11" + KeyManagement.KEY_SEPARATOR + "sk12" + KeyManagement.KEY_END_SEPARATOR, elt.getIdentifierForSecondaryKey(sks.iterator().next()));
	}

	@SecondaryKeys("sk1")
	public static class SubElement extends Element {
		@SecondaryKey("sk1#3") public String sk13;
	}
	
	@Test
	public void skSubclassedIdentifierCalculation() {
		SubElement elt = new SubElement();
		elt.key = "key";
		elt.sk11 = "sk11";
		elt.sk12 = "sk12";
		elt.sk13 = "sk13";

		assertEquals("sk11" + KeyManagement.KEY_SEPARATOR + "sk12" + KeyManagement.KEY_END_SEPARATOR + SubElement.class.getName(), elt.getIdentifierForSecondaryKey(Element.class, "sk1"));
		assertEquals("sk11" + KeyManagement.KEY_SEPARATOR + "sk12" + KeyManagement.KEY_SEPARATOR + "sk13" + KeyManagement.KEY_END_SEPARATOR, elt.getIdentifierForSecondaryKey(SubElement.class, "sk1"));
	}
	
	@Test(expected=IllegalStateException.class)
	public void missingSK() {
		SubElement elt = new SubElement();
		elt.key = "key";
		elt.sk11 = "sk11";
		elt.sk12 = "sk12";
		
		elt.getIdentifierForSecondaryKey(Element.class, "sk2");
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void skSubclassedUnknownIdentifierCalculation() {
		SubElement elt = new SubElement();
		elt.key = "key";
		elt.sk11 = "sk11";
		elt.sk12 = "sk12";
		elt.sk13 = "sk13";
		
		elt.getIdentifierForSecondaryKey(Element.class, "sk2");
	}
	
	@Test(expected=IllegalStateException.class)
	public void skChanged() {
		Element elt = new Element();
		elt.key = "key";
		elt.sk11 = "sk11";
		elt.sk12 = "sk12";

		elt.checkIsValid();

		elt.sk12 = "sk12changed";

		elt.checkIsValid();
	}
	
	@Test(expected=IllegalStateException.class)
	public void skMissing() {
		Element elt = new Element();
		elt.key = "key";
		elt.sk11 = "sk11";

		elt.checkIsValid();
		
	}
	

}
