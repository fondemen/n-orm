package com.googlecode.n_orm;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.googlecode.n_orm.SecondaryKeyDeclaration.SecondaryKeyField;

public class SecondaryKeyDetectionTest {
	@Persisting @SecondaryKeys({"firstindex", "secondindex"})
	public static class Element {
		@Key @SecondaryKey("secondindex#reverted") public String key;
		@SecondaryKey({"firstindex", "secondindex#2#reverted"}) public String att1;
		@SecondaryKey("firstindex#2") public String att2;
	}

	@Test
	public void skDetection() throws SecurityException, NoSuchFieldException {
		Set<SecondaryKeyDeclaration> ids = SecondaryKeyManagement.getInstance().getSecondaryKeyDeclarations(Element.class);
		Field key = Element.class.getDeclaredField("key");
		Field att1 = Element.class.getDeclaredField("att1");
		Field att2 = Element.class.getDeclaredField("att2");
		
		assertEquals(2, ids.size());
		
		boolean foundFirst = false, foundSecond = false;
		for (SecondaryKeyDeclaration id : ids) {

			assertEquals(Element.class, id.getDeclaringClass());
			
			if (id.getName().equals("firstindex")) {
				if (foundFirst)
					fail("firstindex detected twice");
				foundFirst = true;
				
				List<SecondaryKeyField> fis = id.getIndexes();
				
				assertEquals(2, fis.size());
				
				assertEquals(1, fis.get(0).getOrder());
				assertEquals(att1, fis.get(0).getField());
				assertFalse(fis.get(0).isReverted());
				
				assertEquals(2, fis.get(1).getOrder());
				assertEquals(att2, fis.get(1).getField());
				assertFalse(fis.get(1).isReverted());
				
				
			} else if (id.getName().equals("secondindex")) {
				if (foundSecond)
					fail("firstindex detected twice");
				foundSecond = true;
				
				List<SecondaryKeyField> fis = id.getIndexes();
				
				assertEquals(2, fis.size());
				
				assertEquals(1, fis.get(0).getOrder());
				assertEquals(key, fis.get(0).getField());
				assertTrue(fis.get(0).isReverted());
				
				assertEquals(2, fis.get(1).getOrder());
				assertEquals(att1, fis.get(1).getField());
				assertTrue(fis.get(1).isReverted());
				
			} else {
				fail("unknonw index detected: " + id.getName());
			}
		}

		assertTrue(foundFirst);
		assertTrue(foundSecond);
	}
	
	@Persisting @SecondaryKeys("")
	public static class ElementUnnamedIndex {
		@Key @SecondaryKey("") public String key;
	}
	@Test(expected=IllegalArgumentException.class)
	public void indexDetectionUnnamedIndex() {
		SecondaryKeyManagement.getInstance().getSecondaryKeyDeclarations(ElementUnnamedIndex.class);
	}
	
	@Persisting @SecondaryKeys("index")
	public static class ElementMissingIndex {
		@Key @SecondaryKey("secondindex#reverted") public String key;
		@SecondaryKey({"firstindex", "secondindex#2#reverted"}) public String att1;
		@SecondaryKey("firstindex#2") public String att2;
	}
	@Test(expected=IllegalArgumentException.class)
	public void indexDetectionMissingIndex() {
		SecondaryKeyManagement.getInstance().getSecondaryKeyDeclarations(ElementMissingIndex.class);
	}
	
	@Persisting @SecondaryKeys("index")
	public static class ElementMissingOrder {
		@Key @SecondaryKey("index#2") public String key;
	}
	@Test(expected=IllegalArgumentException.class)
	public void indexDetectionMissingOrder() {
		SecondaryKeyManagement.getInstance().getSecondaryKeyDeclarations(ElementMissingOrder.class);
	}
	
	@Persisting @SecondaryKeys("index")
	public static class ElementDuplicateOrder {
		@Key @SecondaryKey("index") public String key;
		@SecondaryKey("index") public String att;
	}
	@Test(expected=IllegalArgumentException.class)
	public void indexDetectionDuplicateOrder() {
		SecondaryKeyManagement.getInstance().getSecondaryKeyDeclarations(ElementDuplicateOrder.class);
	}
	
	@Persisting @SecondaryKeys("index")
	public static class ElementBadOrder {
		@Key @SecondaryKey("index#X") public String key;
	}
	@Test(expected=IllegalArgumentException.class)
	public void indexDetectionBadOrder() {
		SecondaryKeyManagement.getInstance().getSecondaryKeyDeclarations(ElementBadOrder.class);
	}
	
	@Persisting @SecondaryKeys("index")
	public static class ElementBadDeclaration {
		@Key @SecondaryKey("indexdfcqjekfhzj") public String key;
	}
	@Test(expected=IllegalArgumentException.class)
	public void indexDetectionBadDeclaration() {
		SecondaryKeyManagement.getInstance().getSecondaryKeyDeclarations(ElementBadDeclaration.class);
	}

	@SecondaryKeys("firstindex")
	public static class InheritingElement extends Element {
		@SecondaryKey("firstindex#3") public String att3;
	}

	@Test
	public void indexDetectionInherited() throws SecurityException, NoSuchFieldException {
		Set<SecondaryKeyDeclaration> ids = SecondaryKeyManagement.getInstance().getSecondaryKeyDeclarations(InheritingElement.class);
		Field key = Element.class.getDeclaredField("key");
		Field att1 = Element.class.getDeclaredField("att1");
		Field att2 = Element.class.getDeclaredField("att2");
		Field att3 = InheritingElement.class.getDeclaredField("att3");
		
		assertEquals(1, ids.size());
		
		SecondaryKeyDeclaration id  = ids.iterator().next();
		assertEquals("firstindex", id.getName());
				
		List<SecondaryKeyField> fis = id.getIndexes();
		
		assertEquals(3, fis.size());
		
		assertEquals(1, fis.get(0).getOrder());
		assertEquals(att1, fis.get(0).getField());
		assertFalse(fis.get(0).isReverted());
		
		assertEquals(2, fis.get(1).getOrder());
		assertEquals(att2, fis.get(1).getField());
		assertFalse(fis.get(1).isReverted());
		
		assertEquals(3, fis.get(2).getOrder());
		assertEquals(att3, fis.get(2).getField());
		assertFalse(fis.get(2).isReverted());
				
	}
	
	public static class NonPersistingElement {
		@Key @SecondaryKey("secondindex#reverted") public String key;
		@SecondaryKey({"firstindex", "secondindex#2#reverted"}) public String att1;
		@SecondaryKey("firstindex#2") public String att2;
	}

	@Persisting @SecondaryKeys("firstindex")
	public static class InheritingNonPersistingElement extends NonPersistingElement {
		@SecondaryKey("firstindex#3") public String att3;
	}
	@Test
	public void indexDetectionInheritedNonPersisting() throws SecurityException, NoSuchFieldException {
		Set<SecondaryKeyDeclaration> ids = SecondaryKeyManagement.getInstance().getSecondaryKeyDeclarations(InheritingNonPersistingElement.class);
		Field key = NonPersistingElement.class.getDeclaredField("key");
		Field att1 = NonPersistingElement.class.getDeclaredField("att1");
		Field att2 = NonPersistingElement.class.getDeclaredField("att2");
		Field att3 = InheritingNonPersistingElement.class.getDeclaredField("att3");
		
		assertEquals(1, ids.size());
		
		SecondaryKeyDeclaration id  = ids.iterator().next();
		assertEquals("firstindex", id.getName());
				
		List<SecondaryKeyField> fis = id.getIndexes();
		
		assertEquals(3, fis.size());
		
		assertEquals(1, fis.get(0).getOrder());
		assertEquals(att1, fis.get(0).getField());
		assertFalse(fis.get(0).isReverted());
		
		assertEquals(2, fis.get(1).getOrder());
		assertEquals(att2, fis.get(1).getField());
		assertFalse(fis.get(1).isReverted());
		
		assertEquals(3, fis.get(2).getOrder());
		assertEquals(att3, fis.get(2).getField());
		assertFalse(fis.get(2).isReverted());
				
	}
	
	@SecondaryKeys("firstindex")
	public static class InheritingElementDuplicateOrder extends Element {
		@SecondaryKey("firstindex") public String att3;
	}
	@Test(expected=IllegalArgumentException.class)
	public void indexDetectionInheritingDuplicateOrder() {
		SecondaryKeyManagement.getInstance().getSecondaryKeyDeclarations(InheritingElementDuplicateOrder.class);
	}


	@SecondaryKeys("firstindex")
	public static class InheritingPersistingElement extends Element {
		@SecondaryKey("firstindex#3") public String att3;
	}
	@Test
	public void indexDetectionInheritedPersisting() throws SecurityException, NoSuchFieldException {
		Set<SecondaryKeyDeclaration> ids = SecondaryKeyManagement.getInstance().getSecondaryKeyDeclarations(InheritingPersistingElement.class);
		Field key = Element.class.getDeclaredField("key");
		Field att1 = Element.class.getDeclaredField("att1");
		Field att2 = Element.class.getDeclaredField("att2");
		Field att3 = InheritingPersistingElement.class.getDeclaredField("att3");
		
		assertEquals(1, ids.size());
		
		SecondaryKeyDeclaration id  = ids.iterator().next();
		assertEquals("firstindex", id.getName());
		assertEquals(InheritingPersistingElement.class, id.getDeclaringClass());
				
		List<SecondaryKeyField> fis = id.getIndexes();
		
		assertEquals(3, fis.size());
		
		assertEquals(1, fis.get(0).getOrder());
		assertEquals(att1, fis.get(0).getField());
		assertFalse(fis.get(0).isReverted());
		
		assertEquals(2, fis.get(1).getOrder());
		assertEquals(att2, fis.get(1).getField());
		assertFalse(fis.get(1).isReverted());
		
		assertEquals(3, fis.get(2).getOrder());
		assertEquals(att3, fis.get(2).getField());
		assertFalse(fis.get(2).isReverted());
				
	}
}
