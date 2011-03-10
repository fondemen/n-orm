package com.googlecode.n_orm.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.lang.reflect.Field;
import java.util.Map;

import org.junit.Test;

import com.googlecode.n_orm.Constraint;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.query.ClassConstraintBuilder;



public class ConstraintBuilderTest {
	
	@Persisting(table="PersistableSearch")
	public static class SUTClass {
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
}
