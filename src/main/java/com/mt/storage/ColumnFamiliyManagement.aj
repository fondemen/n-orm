package com.mt.storage;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.aspectj.lang.reflect.FieldSignature;

public aspect ColumnFamiliyManagement {

	declare soft : NoSuchFieldException : within(ColumnFamiliyManagement) && adviceexecution();
	declare soft : IllegalAccessException : within(ColumnFamiliyManagement) && adviceexecution();
	declare soft : NoSuchMethodException : within(ColumnFamiliyManagement) && adviceexecution();
	declare soft : InvocationTargetException : within(ColumnFamiliyManagement) && adviceexecution();
	
	declare error: set(!transient !static !final Collection+ PersistingElement+.*) : "A persisting column family must be final";
	
	private transient Map<String, ColumnFamily<?>> PersistingElement.columnFamilies = new HashMap<String, ColumnFamily<?>>();
	
	private void PersistingElement.addColumnFamily(ColumnFamily<?> cf) {
		this.columnFamilies.put(cf.getName(), cf);
	}
	
	public Set<ColumnFamily<?>> PersistingElement.getColumnFamilies() {
		return new HashSet<ColumnFamily<?>>(this.columnFamilies.values());
	}
	
	public ColumnFamily<?> PersistingElement.getColumnFamily(String name) {
		return this.columnFamilies.get(name);
	}
	
	public boolean PersistingElement.hasChanged() {
		for (ColumnFamily<?> cf : this.columnFamilies.values()) {
			if (cf.hasChanged())
				return true;
		}
		return false;
	}
	
	

	public void PersistingElement.activate(String name, Object startIndex, Object endIndex) throws DatabaseNotReachedException {
		ColumnFamily<?> cf = this.getColumnFamily(name);
		if (cf == null) {
			throw new IllegalArgumentException(name + " unknown column family.");
		} else {
			cf.activate(startIndex, endIndex);
		}
	}
	
	public boolean isCollectionFamily(Field f) {
		try {
			this.checkCollectionFamily(f);
			return true;
		} catch (IllegalStateException x) {
			return false;
		}
	}
	
	public void checkCollectionFamily(Field f) {
		if ((f.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) != 0)
			return;
		
		if (! this.isCollectionType(f.getType()))
			throw new IllegalStateException("The "
							+ f
							+ " key should be of a collection type");
	}

	public boolean isCollectionType(Class<?> type) {
		return Collection.class.equals(type) || ColumnFamily.class.equals(type);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void around(PersistingElement self, Collection<?> coll): set(!transient !static final Collection+ PersistingElement+.*) && target(self) && args(coll) {
		if (coll != null && (coll instanceof ColumnFamily<?>))
			proceed(self, coll);
		FieldSignature sign = (FieldSignature)thisJoinPointStaticPart.getSignature();
		Field field = sign.getField();
		assert isCollectionFamily(field);
		ParameterizedType collType = (ParameterizedType) field.getGenericType();
		Class<?> elementClass = (Class)collType.getActualTypeArguments()[0];
		Indexed index = field.getAnnotation(Indexed.class);
		if (index == null)
			throw new IllegalArgumentException("Field " + field + " must declare annotation " + Indexed.class);
		if (coll != null)
			throw new IllegalArgumentException("Can only set null value to persisting collection " + field);
		Incrementing incr = field.getAnnotation(Incrementing.class);
		proceed(self, new ColumnFamily(elementClass, field.getName(), self, index.field(), field.isAnnotationPresent(AddOnly.class), field.isAnnotationPresent(Incrementing.class)));
	}
	
	after(ColumnFamily cf) returning : execution(ColumnFamily.new(..)) && target(cf) {
		cf.getOwner().addColumnFamily(cf);
	}
}
