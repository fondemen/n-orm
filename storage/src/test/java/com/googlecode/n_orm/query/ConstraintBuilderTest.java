package com.googlecode.n_orm.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.query.ClassConstraintBuilder;
import com.googlecode.n_orm.storeapi.Constraint;



public class ConstraintBuilderTest {
	
	@Persisting(table="PersistableSearch")
	public static class SUTClass {
		private static final long serialVersionUID = 1L;
		@Key public int key1;
		@Key(order=2) public int key2;
		
		public int dummyVar;
		
		public SUTClass(int key1, int key2) {
			this.key1 = key1;
			this.key2 = key2;
		}
		
		public boolean equals(Object rhs) {
			return rhs != null && rhs.getClass().equals(SUTClass.class) && ((SUTClass)rhs).key1 == this.key1 && ((SUTClass)rhs).key2 == this.key2;
		}
		
		
	}
	
	
	@Test
	public void completeSearch() {
		ClassConstraintBuilder<SUTClass> c = StorageManagement.findElements().ofClass(SUTClass.class).withKey("key1").setTo(1).andWithKey("key2").between(2).and(3).withAtMost(4).elements();
		assertEquals(c.getClazz(), SUTClass.class);
		assertEquals(c.getSearchedKey().getName(), "key2");
		assertEquals(c.getSearchFrom(), 2);
		assertEquals(c.getSearchTo(), 3);
		Map<Field, Object> keyValues = c.getKeyValues();
		assertEquals(1, keyValues.keySet().size());
		Field key = keyValues.keySet().iterator().next();
		assertEquals("key1", key.getName());
		assertEquals(1, keyValues.get(key));
	}
	
	@Test(expected=IllegalStateException.class)
	public void lacksLimit() throws DatabaseNotReachedException {
		StorageManagement.findElements().ofClass(SUTClass.class).withKey("key1").setTo(1).andWithKey("key2").between(2).and(3).go();
	}
	
	@Test
	public void stringParameter() throws DatabaseNotReachedException {
		 Constraint c = StorageManagement.findElements().ofClass(SUTClass.class).withKey("key1").setTo("765").getConstraint();
		 assertEquals(ConversionTools.convertToString(765) + KeyManagement.KEY_SEPARATOR, c.getStartKey());
	}
	
	@Test
	public void lacksSearchKey() throws DatabaseNotReachedException {
		ClassConstraintBuilder<SUTClass> c = StorageManagement.findElements().ofClass(SUTClass.class).withKey("key1").setTo(1).withAtMost(4).elements();
		assertEquals(c.getClazz(), SUTClass.class);
		assertNull(c.getSearchedKey());
		assertNull(c.getSearchFrom());
		assertNull(c.getSearchTo());
		Map<Field, Object> keyValues = c.getKeyValues();
		assertEquals(1, keyValues.keySet().size());
		Field key = keyValues.keySet().iterator().next();
		assertEquals("key1", key.getName());
		assertEquals(1, keyValues.get(key));
		Constraint cs = c.getConstraint();
		assertEquals(ConversionTools.convertToString(1, int.class)+KeyManagement.KEY_SEPARATOR, cs.getStartKey());
	}
	
	@Test
	public void lacksPreviousKey() {
		ClassConstraintBuilder<SUTClass> c = StorageManagement.findElements().ofClass(SUTClass.class).andWithKey("key2").between(2).and(3).withAtMost(4).elements();
		assertEquals(c.getClazz(), SUTClass.class);
		assertEquals(c.getSearchedKey().getName(), "key2");
		assertEquals(c.getSearchFrom(), 2);
		assertEquals(c.getSearchTo(), 3);
		Map<Field, Object> keyValues = c.getKeyValues();
		assertEquals(0, keyValues.keySet().size());
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void lacksPreviousKeyConstraint() {
		StorageManagement.findElements().ofClass(SUTClass.class).andWithKey("key2").between(2).and(3).withAtMost(4).elements().getConstraint();
		
	}
	
	@Persisting
	public static class SUTOuterClass {
		private static final long serialVersionUID = 1L;
		public @Key SUTClass key1;
		public @Key(order=2) SUTClass key2;
	}
	
	@Test
	public void completeSearchDivePartial() {
		InnerClassConstraintBuilder<SUTOuterClass> co = StorageManagement.findElements().ofClass(SUTOuterClass.class).withKey("key1").isAnElement().withKey("key1").setTo(1).andWithKey("key2").between(2).and(3);
		assertEquals(co.getClazz(), SUTClass.class);
		assertEquals(co.getSearchedKey().getName(), "key2");
		assertEquals(co.getSearchedKey().getType(), int.class);
		assertEquals(2, co.getSearchFrom());
		assertEquals(3, co.getSearchTo());
		Map<Field, Object> keyValues = co.getKeyValues();
		assertEquals(1, keyValues.keySet().size());
		Entry<Field, Object> kv = keyValues.entrySet().iterator().next();
		assertEquals("key1", kv.getKey().getName());
		assertEquals(int.class,kv.getKey().getType());
		assertEquals(1, kv.getValue());
		
	}
	
	@Test
	public void completeSearchDive() {
		ClassConstraintBuilder<SUTOuterClass> co = StorageManagement.findElements().ofClass(SUTOuterClass.class).withKey("key1").isAnElement().withKey("key1").setTo(1).andWithKey("key2").between(2).and(3).and().withAtMost(4).elements();
		assertEquals(co.getClazz(), SUTOuterClass.class);
		assertEquals(co.getSearchedKey().getName(), "key1");
		assertEquals(co.getSearchedKey().getType(), SUTClass.class);
		assertNull(co.getSearchFrom());
		assertNull(co.getSearchTo());
		Map<Field, Object> keyValues = co.getKeyValues();
		assertEquals(0, keyValues.keySet().size());
		
		Constraint subcstr = co.getConstraint();
		assertEquals(ConversionTools.convertToString(1) + KeyManagement.KEY_SEPARATOR + ConversionTools.convertToString(2), subcstr.getStartKey());
		assertEquals(ConversionTools.convertToString(1) + KeyManagement.KEY_SEPARATOR + ConversionTools.convertToString(4), subcstr.getEndKey());
	}
	
	public static enum AnEnum {
		EV1,EV2
	}
	
	@Persisting
	public static class AnElementWithEnumKey {
		private static final long serialVersionUID = -6188603495123606070L;
		@Key public AnEnum key;
	}
	
	@Test
	public void findElementWithEnumKeyGivenAsString() {
		Constraint c = StorageManagement.findElements().ofClass(AnElementWithEnumKey.class).withKey("key").setTo("EV2").getConstraint();
		assertEquals(c.getStartKey(), AnEnum.EV2.name()+KeyManagement.KEY_END_SEPARATOR);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void findElementWithBadEnumKeyGivenAsString() {
		StorageManagement.findElements().ofClass(AnElementWithEnumKey.class).withKey("key").setTo("XXXEV2").getConstraint();
	}
}
