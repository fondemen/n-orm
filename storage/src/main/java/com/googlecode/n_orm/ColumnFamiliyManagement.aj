package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.aspectj.lang.reflect.FieldSignature;

import com.googlecode.n_orm.AddOnly;
import com.googlecode.n_orm.ColumnFamiliyManagement;
import com.googlecode.n_orm.Incrementing;
import com.googlecode.n_orm.Indexed;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.cf.MapColumnFamily;
import com.googlecode.n_orm.cf.SetColumnFamily;


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

	declare error: set(!transient !static final (Set || Map || ColumnFamily+) PersistingElement+.*) : "A persisting column family must not be final";
	declare error: set(static (Set || Map || ColumnFamily+) PersistingElement+.*) : "Column families must not be static";
	declare error: set(!transient !static (Collection+ && !Set && !ColumnFamily+) PersistingElement+.*) : "Only Set and Maps are supported collections";

	declare warning: get(@ImplicitActivation transient * PersistingElement+.*)
		|| get(@ImplicitActivation static * PersistingElement+.*) : "This field is not persitent, thus cannot be auto-activated";
	
	private transient Map<String, ColumnFamily<?>> PersistingElement.columnFamilies = new TreeMap<String, ColumnFamily<?>>();
	
	private Map<String, ColumnFamily<?>> PersistingElement.getColumnFamiliesInt() {
		if (this.columnFamilies == null) {
			this.columnFamilies = new TreeMap<String, ColumnFamily<?>>();
		}
		return this.columnFamilies;
	}
	
	private void PersistingElement.addColumnFamily(ColumnFamily<?> cf) {
		this.getColumnFamiliesInt().put(cf.getName(), cf);
	}
	
	public Set<ColumnFamily<?>> PersistingElement.getColumnFamilies() {
		return new HashSet<ColumnFamily<?>>(this.getColumnFamiliesInt().values());
	}
	
	public ColumnFamily<?> PersistingElement.getColumnFamily(String name) throws UnknownColumnFamily {
		ColumnFamily<?> ret = this.getColumnFamiliesInt().get(name);
		if (ret == null)
			throw new UnknownColumnFamily(this.getClass(), name);
		else
			return ret;
	}
	
	//For test purpose
	void PersistingElement.clearColumnFamilies() {
		this.getColumnFamiliesInt().clear();
	}
	
	public boolean PersistingElement.hasChanged() {
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
		return ((f.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) == 0) && this.isCollectionType(f.getType());
	}

	/**
	 * Checks whether a column family attribute can have this type
	 */
	public boolean isCollectionType(Class<?> type) {
		return Set.class.equals(type) || Map.class.equals(type) || SetColumnFamily.class.isAssignableFrom(type) || MapColumnFamily.class.isAssignableFrom(type);
	}
	
	/**
	 * Finds in a class the fields that are column families
	 */
	public Set<Field> detectColumnFamilies(Class<? extends PersistingElement> clazz) {
		Set<Field> ret = new HashSet<Field>();
		Class<?> c = clazz;
		do {
			for (Field field : c.getDeclaredFields()) {
				if (isCollectionFamily(field))
					ret.add(field);
			}
			c = c.getSuperclass();
		} while (PersistingElement.class.isAssignableFrom(c));
		return ret;
	}
	
	private boolean PersistingElement.inPOJOMode = false;
	
	/**
	 * Sets this object in POJO mode or not. POJO mode makes all non final static or transient fields
	 * simple elements, i.e. with no reference to ColumnFamily or one of its subclass. This is performed
	 * a transitive way, which means that all referenced persisting elements will also be set to the
	 * requested POJO mode. The main interest of this method is for serializing Persisting elements:
	 * a persisting element should be set into POJO mode before being serialized, and into normal mode
	 * once deserialized.
	 * @param pojo
	 */
	public void PersistingElement.setPOJO(boolean pojo) {
		if (this.inPOJOMode == pojo)
			return;
		PropertyManagement pm = PropertyManagement.getInstance();
		Map<PersistingElement, Map<Field, Object>> toBeSet = new HashMap<PersistingElement, Map<Field,Object>>();
		this.grabSetPOJOAtts(pojo, toBeSet);
		for (Entry<PersistingElement, Map<Field, Object>> pe : toBeSet.entrySet()) {
			for (Entry<Field, Object> prop : pe.getValue().entrySet()) {
				pm .candideSetValue(pe.getKey(), prop.getKey(), prop.getValue());
			}
		}
	}
		
	private void PersistingElement.grabSetPOJOAtts(boolean pojo, Map<PersistingElement, Map<Field, Object>> toBeSet) {
		if (this.inPOJOMode == pojo)
			return;
		this.inPOJOMode = pojo;
		Map<Field, Object> thisToBeSet = new HashMap<Field, Object>();
		ColumnFamiliyManagement cfm = ColumnFamiliyManagement.getInstance();
		for (Field f : cfm.detectColumnFamilies(this.getClass())) {
			ColumnFamily<?> cf = this.getColumnFamiliesInt().get(f.getName());
			if (cf == null) {
				cf = cfm.createColumnFamily(this, f);
			}
			if (cf.getClazz().isAssignableFrom(PersistingElement.class) || PersistingElement.class.isAssignableFrom(cf.getClazz())) {
				for (String k : cf.getKeys()) {
					Object o = cf.getElement(k);
					if (o instanceof PersistingElement) {
						((PersistingElement)o).grabSetPOJOAtts(pojo, toBeSet);
					}
				}
			}
			Field prop = cf.getProperty();
			if (prop != null) {
				Object val = pojo ? cf.getSerializableVersion() : cf;
				if (prop.getType().isInstance(val)) {
					thisToBeSet.put(prop, val);
				} else {
					throw new ClassCastException("Cannot set " + prop + " to " + val + " in " + this);
				}
			}
		}
		toBeSet.put(this, thisToBeSet);
	}
	
	before(PersistingElement self): execution(void PersistingElement+.activate(String...)) && this(self) {
		self.setPOJO(false);
	}
	
	before(PersistingElement self): execution(void PersistingElement+.store()) && this(self) {
		if (self.inPOJOMode)
			throw new IllegalStateException("Cannot store a persisting element in POJO mode ; call setPOJO(false) before on " + self);
	}
	
	void around(PersistingElement self, Object cf): set(!transient !static (Set+ || Map+) PersistingElement+.*) && target(self) && args(cf) {

		FieldSignature sign = (FieldSignature)thisJoinPointStaticPart.getSignature();
		Field field = sign.getField();
		assert isCollectionFamily(field);
		
		if (cf != null && (cf instanceof ColumnFamily<?>)) {
			proceed(self, cf);
			return;
		}
		if (cf != null)
			throw new IllegalArgumentException("Can only set null value to persisting collection " + field);

		createColumnFamily(self, field);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private ColumnFamily<?> createColumnFamily(PersistingElement self, Field field) {
		PropertyManagement pm = PropertyManagement.getInstance();
		Object oldCf = pm.candideReadValue(self, field);
		if (oldCf != null && oldCf instanceof ColumnFamily<?>)
			throw new IllegalArgumentException("Cannot change column family " + field + " as it is already set in " + self);
		Indexed index = field.getAnnotation(Indexed.class);
		ColumnFamily<?> acf;
		ParameterizedType collType = (ParameterizedType) field.getGenericType();
		if (Map.class.isAssignableFrom(field.getType())) {
			if (index != null)
				throw new IllegalArgumentException("Map " + field + " cannot declare annotation " + Indexed.class);
			Class<?> keyClass = (Class<?>)collType.getActualTypeArguments()[0], valueClass = (Class<?>)collType.getActualTypeArguments()[1];
			acf = new MapColumnFamily(keyClass, valueClass, field, field.getName(), self, field.isAnnotationPresent(AddOnly.class), field.isAnnotationPresent(Incrementing.class));
			if (oldCf != null) {
				for (Entry<?, ?> e : ((Map<?,?>)oldCf).entrySet()) {
					((MapColumnFamily)acf).put(e.getKey(), e.getValue());
				}
			}
		} else {
			if (index == null)
				throw new IllegalArgumentException("Field " + field + " must declare annotation " + Indexed.class);
			Class<?> elementClass = (Class<?>)collType.getActualTypeArguments()[0];
			try {
				acf = new SetColumnFamily(elementClass, field, self, index.field(), field.isAnnotationPresent(AddOnly.class));
				if (oldCf != null) {
					for (Object e : (Set<?>)oldCf) {
						((SetColumnFamily)acf).add(e);
					}
				}
			} catch (NoSuchFieldException x) {
				throw new RuntimeException(x);
			}
		}
		pm.candideSetValue(self, field, acf);
		return acf;
	}
	
	after(ColumnFamily cf) returning : execution(ColumnFamily.new(..)) && target(cf) {
		cf.getOwner().addColumnFamily(cf);
	}
}
