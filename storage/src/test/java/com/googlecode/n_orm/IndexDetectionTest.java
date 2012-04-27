package com.googlecode.n_orm;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.googlecode.n_orm.IndexDeclaration.IndexField;

public class IndexDetectionTest {
	@Persisting @Indexable({"firstindex", "secondindex"})
	public static class Element {
		@Key @Index("secondindex#reverted") public String key;
		@Index({"firstindex", "secondindex#2#reverted"}) public String att1;
		@Index("firstindex#2") public String att2;
	}

	@Test
	public void indexDetection() throws SecurityException, NoSuchFieldException {
		Set<IndexDeclaration> ids = IndexManagement.getInstance().getIndexDeclarations(Element.class);
		Field key = Element.class.getDeclaredField("key");
		Field att1 = Element.class.getDeclaredField("att1");
		Field att2 = Element.class.getDeclaredField("att2");
		
		assertEquals(2, ids.size());
		
		boolean foundFirst = false, foundSecond = false;
		for (IndexDeclaration id : ids) {

			assertEquals(Element.class, id.getDeclaringClass());
			
			if (id.getName().equals("firstindex")) {
				if (foundFirst)
					fail("firstindex detected twice");
				foundFirst = true;
				
				List<IndexField> fis = id.getIndexes();
				
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
				
				List<IndexField> fis = id.getIndexes();
				
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
	
	@Persisting @Indexable("")
	public static class ElementUnnamedIndex {
		@Key @Index("") public String key;
	}
	@Test(expected=IllegalArgumentException.class)
	public void indexDetectionUnnamedIndex() {
		IndexManagement.getInstance().getIndexDeclarations(ElementUnnamedIndex.class);
	}
	
	@Persisting @Indexable("index")
	public static class ElementMissingIndex {
		@Key @Index("secondindex#reverted") public String key;
		@Index({"firstindex", "secondindex#2#reverted"}) public String att1;
		@Index("firstindex#2") public String att2;
	}
	@Test(expected=IllegalArgumentException.class)
	public void indexDetectionMissingIndex() {
		IndexManagement.getInstance().getIndexDeclarations(ElementMissingIndex.class);
	}
	
	@Persisting @Indexable("index")
	public static class ElementMissingOrder {
		@Key @Index("index#2") public String key;
	}
	@Test(expected=IllegalArgumentException.class)
	public void indexDetectionMissingOrder() {
		IndexManagement.getInstance().getIndexDeclarations(ElementMissingOrder.class);
	}
	
	@Persisting @Indexable("index")
	public static class ElementDuplicateOrder {
		@Key @Index("index") public String key;
		@Index("index") public String att;
	}
	@Test(expected=IllegalArgumentException.class)
	public void indexDetectionDuplicateOrder() {
		IndexManagement.getInstance().getIndexDeclarations(ElementDuplicateOrder.class);
	}
	
	@Persisting @Indexable("index")
	public static class ElementBadOrder {
		@Key @Index("index#X") public String key;
	}
	@Test(expected=IllegalArgumentException.class)
	public void indexDetectionBadOrder() {
		IndexManagement.getInstance().getIndexDeclarations(ElementBadOrder.class);
	}
	
	@Persisting @Indexable("index")
	public static class ElementBadDeclaration {
		@Key @Index("indexdfcqjekfhzj") public String key;
	}
	@Test(expected=IllegalArgumentException.class)
	public void indexDetectionBadDeclaration() {
		IndexManagement.getInstance().getIndexDeclarations(ElementBadDeclaration.class);
	}

	@Indexable("firstindex")
	public static class InheritingElement extends Element {
		@Index("firstindex#3") public String att3;
	}

	@Test
	public void indexDetectionInherited() throws SecurityException, NoSuchFieldException {
		Set<IndexDeclaration> ids = IndexManagement.getInstance().getIndexDeclarations(InheritingElement.class);
		Field key = Element.class.getDeclaredField("key");
		Field att1 = Element.class.getDeclaredField("att1");
		Field att2 = Element.class.getDeclaredField("att2");
		Field att3 = InheritingElement.class.getDeclaredField("att3");
		
		assertEquals(1, ids.size());
		
		IndexDeclaration id  = ids.iterator().next();
		assertEquals("firstindex", id.getName());
				
		List<IndexField> fis = id.getIndexes();
		
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
		@Key @Index("secondindex#reverted") public String key;
		@Index({"firstindex", "secondindex#2#reverted"}) public String att1;
		@Index("firstindex#2") public String att2;
	}

	@Persisting @Indexable("firstindex")
	public static class InheritingNonPersistingElement extends NonPersistingElement {
		@Index("firstindex#3") public String att3;
	}
	@Test
	public void indexDetectionInheritedNonPersisting() throws SecurityException, NoSuchFieldException {
		Set<IndexDeclaration> ids = IndexManagement.getInstance().getIndexDeclarations(InheritingNonPersistingElement.class);
		Field key = NonPersistingElement.class.getDeclaredField("key");
		Field att1 = NonPersistingElement.class.getDeclaredField("att1");
		Field att2 = NonPersistingElement.class.getDeclaredField("att2");
		Field att3 = InheritingNonPersistingElement.class.getDeclaredField("att3");
		
		assertEquals(1, ids.size());
		
		IndexDeclaration id  = ids.iterator().next();
		assertEquals("firstindex", id.getName());
				
		List<IndexField> fis = id.getIndexes();
		
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
	
	@Indexable("firstindex")
	public static class InheritingElementDuplicateOrder extends Element {
		@Index("firstindex") public String att3;
	}
	@Test(expected=IllegalArgumentException.class)
	public void indexDetectionInheritingDuplicateOrder() {
		IndexManagement.getInstance().getIndexDeclarations(InheritingElementDuplicateOrder.class);
	}


	@Indexable("firstindex")
	public static class InheritingPersistingElement extends Element {
		@Index("firstindex#3") public String att3;
	}
	@Test
	public void indexDetectionInheritedPersisting() throws SecurityException, NoSuchFieldException {
		Set<IndexDeclaration> ids = IndexManagement.getInstance().getIndexDeclarations(InheritingPersistingElement.class);
		Field key = Element.class.getDeclaredField("key");
		Field att1 = Element.class.getDeclaredField("att1");
		Field att2 = Element.class.getDeclaredField("att2");
		Field att3 = InheritingPersistingElement.class.getDeclaredField("att3");
		
		assertEquals(1, ids.size());
		
		IndexDeclaration id  = ids.iterator().next();
		assertEquals("firstindex", id.getName());
		assertEquals(InheritingPersistingElement.class, id.getDeclaringClass());
				
		List<IndexField> fis = id.getIndexes();
		
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
