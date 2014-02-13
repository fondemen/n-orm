package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.aspectj.lang.reflect.FieldSignature;

import com.googlecode.n_orm.ColumnFamiliyManagement;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.cf.MapColumnFamily;
import com.googlecode.n_orm.cf.SetColumnFamily;
import com.googlecode.n_orm.consoleannotations.Continuator;


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

	declare error: set(!@Transient !transient !static final (Set || Map || ColumnFamily+) PersistingElement+.*) : "A persisting column family must not be final";
	declare error: set(static (Set || Map || ColumnFamily+) PersistingElement+.*) : "Column families must not be static";
	declare error: set(!@Transient !transient !static (Collection+ && !Set && !ColumnFamily+) PersistingElement+.*) : "Only Set and Maps are supported collections";

	declare warning: get(@ImplicitActivation transient * PersistingElement+.*)
		|| get(@ImplicitActivation static * PersistingElement+.*)
		|| get(@ImplicitActivation @Transient * PersistingElement+.*)
		: "This field is not persitent, thus cannot be auto-activated";
	
	private transient Map<String, ColumnFamily<?>> PersistingElement.columnFamilies;
	
	private Map<String, ColumnFamily<?>> PersistingElement.getColumnFamiliesInt() {
		if (this.columnFamilies == null) {
			this.columnFamilies = new TreeMap<String, ColumnFamily<?>>();
			this.getPropertiesColumnFamily();
		}
		return this.columnFamilies;
	}
	
	private void PersistingElement.addColumnFamily(ColumnFamily<?> cf) {
		this.getColumnFamiliesInt().put(cf.getName(), cf);
	}
	
	public Collection<ColumnFamily<?>> PersistingElement.getColumnFamilies() {
		this.getPropertiesColumnFamily();
		return Collections.unmodifiableCollection(this.getColumnFamiliesInt().values());
	}
	
	public Set<String> PersistingElement.getColumnFamilyNames() {
		return Collections.unmodifiableSet(this.getColumnFamiliesInt().keySet());
	}
	
	@Continuator
	public ColumnFamily<?> PersistingElement.getColumnFamily(String name) throws UnknownColumnFamily {
		ColumnFamily<?> ret = this.getColumnFamiliesInt().get(name);
		if (ret == null)
			throw new UnknownColumnFamily(this.getClass(), name);
		else
			return ret;
	}
	
	public ColumnFamily<?> PersistingElement.getColumnFamily(Object collection) throws UnknownColumnFamily {

		if (collection instanceof String) {
			try {
				return this.getColumnFamily((String)collection);
			} catch (Exception x) {}
		}
		
		PropertyManagement pm = PropertyManagement.getInstance();
		for (ColumnFamily<?> cf : this.getColumnFamilies()) {
			try {
				if (cf == collection || pm.readValue(this, cf.getProperty()) == collection)
					return cf;
			} catch (Exception e) {
			}
		}
		throw new UnknownColumnFamily(this.getClass(), collection.toString());
	}
	
	//For test purpose
	void PersistingElement.clearColumnFamilies() {
		this.getColumnFamiliesInt().clear();
	}
	
	public boolean PersistingElement.hasChanged() {
		this.updateFromPOJO();
		for (ColumnFamily<?> cf : this.getColumnFamiliesInt().values()) {
			if (cf.hasChanged())
				return true;
		}
		return false;
	}
	
	/**
	 * Checks whether a {@link Field} of this persisting element represents a column family.
	 */
	public boolean isCollectionFamily(Field f) {
		return ((f.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) == 0) && !f.isAnnotationPresent(Transient.class) && this.isCollectionType(f.getType());
	}

	/**
	 * Checks whether a column family attribute can have this type
	 */
	public boolean isCollectionType(Class<?> type) {
		return Set.class.equals(type) || Map.class.equals(type) || SetColumnFamily.class.isAssignableFrom(type) || MapColumnFamily.class.isAssignableFrom(type);
	}

	private Map<Class<?>, Map<String, Field>> typeColumnFamilies = new HashMap<Class<?>, Map<String, Field>>();
	
	/**
	 * Finds in a class the fields that are column families
	 */
	public Map<String, Field> getColumnFamilies(Class<? extends PersistingElement> clazz) {
		synchronized (typeColumnFamilies) {
			Map<String, Field> ret = this.typeColumnFamilies.get(clazz);
			if (ret == null) {
				ret = new TreeMap<String, Field>();
				Class<?> c = clazz;
				do {
					for (Field field : c.getDeclaredFields()) {
						if (isCollectionFamily(field))
							ret.put(field.getName(), field);
					}
					c = c.getSuperclass();
				} while (c != null);
				typeColumnFamilies.put(clazz, ret);
			}
			return ret;
		}
	}
	

	public Set<Field> getColumnFamilies(Class<? extends PersistingElement> clazz, Set<String> columnFamilyNames) {
		Map<String, Field> cfs = this.getColumnFamilies(clazz);
		Set<Field> ret = new HashSet<Field>();
		for (String fieldName : columnFamilyNames) {
			Field f = cfs.get(fieldName);
			if (f == null)
				throw new IllegalArgumentException("Cannot find column family " + fieldName + " in " + clazz.getName());
			ret.add(f);
		}
		return ret;
	}
	
	public void PersistingElement.updateFromPOJO() {
		for (ColumnFamily<?> cf : this.getColumnFamiliesInt().values()) {
			cf.updateFromPOJO();
		}
	}
	
	void around(PersistingElement self, Object cf): set(!@Transient !transient !static (Set+ || Map+) (*.*)) && !within(ColumnFamiliyManagement) && target(self) && args(cf) {
		FieldSignature sign = (FieldSignature)thisJoinPointStaticPart.getSignature();
		Field field = sign.getField();
		assert isCollectionFamily(field);

		ColumnFamily<?> ccf = createColumnFamily((PersistingElement)self, field, cf);
		
		if(ColumnFamily.class.isAssignableFrom(field.getType()))
			proceed(self, ccf);
		else if (cf == null)
			proceed(self, ccf.getSerializableVersion());
		else
			proceed(self, cf);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private ColumnFamily<?> createColumnFamily(PersistingElement self, Field field, Object oldCf) {
		ColumnFamily<?> acf;
		ParameterizedType collType = (ParameterizedType) field.getGenericType();
		if (Map.class.isAssignableFrom(field.getType())) {
			Class<?> keyClass = (Class<?>)collType.getActualTypeArguments()[0], valueClass = (Class<?>)collType.getActualTypeArguments()[1];
			acf = new MapColumnFamily(keyClass, valueClass, field, field.getName(), self);
			if (oldCf != null) {
				for (Entry<?, ?> e : ((Map<?,?>)oldCf).entrySet()) {
					((MapColumnFamily)acf).put(e.getKey(), e.getValue());
				}
			}
		} else {
			Class<?> elementClass = (Class<?>)collType.getActualTypeArguments()[0];
			try {
				acf = new SetColumnFamily(elementClass, field, self);
				if (oldCf != null) {
					for (Object e : (Set<?>)oldCf) {
						((SetColumnFamily)acf).add(e);
					}
				}
			} catch (NoSuchFieldException x) {
				throw new RuntimeException(x);
			}
		}
		return acf;
	}
	
	after(@SuppressWarnings("rawtypes") ColumnFamily cf) returning : execution(ColumnFamily.new(..)) && target(cf) {
		if (cf.getOwner() != null)
			cf.getOwner().addColumnFamily(cf);
	}
}
