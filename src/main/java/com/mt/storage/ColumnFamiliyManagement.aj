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

import com.mt.storage.cf.CollectionColumnFamily;
import com.mt.storage.cf.ColumnFamily;
import com.mt.storage.cf.MapColumnFamily;
import com.mt.storage.conversion.ConversionTools;


public aspect ColumnFamiliyManagement {
	private static ColumnFamiliyManagement INSTANCE;
	
	public static ColumnFamiliyManagement getInstance() {
		if (INSTANCE == null)
			INSTANCE = aspectOf();
		return INSTANCE;
	}

	declare soft : NoSuchFieldException : within(ColumnFamiliyManagement) && adviceexecution();
	declare soft : IllegalAccessException : within(ColumnFamiliyManagement) && adviceexecution();
	declare soft : NoSuchMethodException : within(ColumnFamiliyManagement) && adviceexecution();
	declare soft : InvocationTargetException : within(ColumnFamiliyManagement) && adviceexecution();

	declare error: set(!transient !static !final Collection+ PersistingElement+.*) : "A persisting column family must be final";
	declare error: set(!transient !static !final Map+ PersistingElement+.*) : "A persisting column family must be final";
	declare error: set(!transient !static !final ColumnFamily+ PersistingElement+.*) : "A persisting column family must be final";
	declare error: set(static ColumnFamily+ PersistingElement+.*) : "Column families must not be static";

	declare warning: get(@ImplicitActivation transient * PersistingElement+.*)
		|| get(@ImplicitActivation static * PersistingElement+.*) : "This field is not persitent, thus cannot be auto-activated";
	
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
		return Collection.class.equals(type) || Map.class.equals(type) || ColumnFamily.class.isAssignableFrom(type);
	}
	
	void around(PersistingElement self, Object cf): (set(!transient !static final Collection+ PersistingElement+.*) || set(!transient !static final Map+ PersistingElement+.*)) && target(self) && args(cf) {
		if (cf != null && (cf instanceof ColumnFamily<?>)) {
			proceed(self, cf);
			return;
		}
		
		FieldSignature sign = (FieldSignature)thisJoinPointStaticPart.getSignature();
		Field field = sign.getField();
		assert isCollectionFamily(field);
		Indexed index = field.getAnnotation(Indexed.class);
		if (cf != null)
			throw new IllegalArgumentException("Can only set null value to persisting collection " + field);
		ColumnFamily<?> acf;
		ParameterizedType collType = (ParameterizedType) field.getGenericType();
		if (Map.class.isAssignableFrom(field.getType())) {
			if (index != null)
				throw new IllegalArgumentException("Map " + field + " cannot declare annotation " + Indexed.class);
			Class<?> keyClass = (Class<?>)collType.getActualTypeArguments()[0], valueClass = (Class<?>)collType.getActualTypeArguments()[1];
			acf = new MapColumnFamily(keyClass, valueClass, field, field.getName(), self, field.isAnnotationPresent(AddOnly.class), field.isAnnotationPresent(Incrementing.class));
		} else {
			if (index == null)
				throw new IllegalArgumentException("Field " + field + " must declare annotation " + Indexed.class);
			Class<?> elementClass = (Class<?>)collType.getActualTypeArguments()[0];
			acf = new CollectionColumnFamily(elementClass, field, self, index.field(), field.isAnnotationPresent(AddOnly.class));
		}
		proceed(self, acf);
	}
	
	after(ColumnFamily cf) returning : execution(ColumnFamily.new(..)) && target(cf) {
		cf.getOwner().addColumnFamily(cf);
	}
}
