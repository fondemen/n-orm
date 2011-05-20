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

import com.googlecode.n_orm.ColumnFamiliyManagement;
import com.googlecode.n_orm.Incrementing;
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

	declare error: set(!@Transient !transient !static final (Set || Map || ColumnFamily+) PersistingElement+.*) : "A persisting column family must not be final";
	declare error: set(static (Set || Map || ColumnFamily+) PersistingElement+.*) : "Column families must not be static";
	declare error: set(!@Transient !transient !static (Collection+ && !Set && !ColumnFamily+) PersistingElement+.*) : "Only Set and Maps are supported collections";

	declare warning: get(@ImplicitActivation transient * PersistingElement+.*)
		|| get(@ImplicitActivation static * PersistingElement+.*)
		|| get(@ImplicitActivation @Transient * PersistingElement+.*)
		: "This field is not persitent, thus cannot be auto-activated";
	
	private transient Map<String, ColumnFamily<?>> PersistingElement.columnFamilies = null;
	
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
	
	public Set<ColumnFamily<?>> PersistingElement.getColumnFamilies() {
		this.getPropertiesColumnFamily();
		return new HashSet<ColumnFamily<?>>(this.getColumnFamiliesInt().values());
	}
	
	public ColumnFamily<?> PersistingElement.getColumnFamily(String name) throws UnknownColumnFamily {
		ColumnFamily<?> ret = this.getColumnFamiliesInt().get(name);
		if (ret == null)
			throw new UnknownColumnFamily(this.getClass(), name);
		else
			return ret;
	}
	
	public ColumnFamily<?> PersistingElement.getColumnFamily(Object collection) throws UnknownColumnFamily {
		PropertyManagement pm = PropertyManagement.getInstance();
		for (ColumnFamily<?> cf : this.getColumnFamiliesInt().values()) {
			try {
				if (pm.readValue(this, cf.getProperty()) == collection)
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
	
	//@Transient private boolean PersistingElement.inPOJOMode = false; //Must be @Transient and not transient: in case the persisting element was serialized in pojo mode, it must be deserialized in pojo mode
	
	/**
	 * @deprecated already working in POJO mode ; this method is just useless
	 * Sets this object in POJO mode or not. POJO mode makes all non final static or transient fields
	 * simple elements, i.e. with no reference to ColumnFamily or one of its subclass. This is performed
	 * a transitive way, which means that all referenced persisting elements will also be set to the
	 * requested POJO mode. The main interest of this method is for serializing Persisting elements:
	 * a persisting element should be set into POJO mode before being serialized, and into normal mode
	 * once deserialized.
	 * @param pojo
	 */
	@Deprecated
	public void PersistingElement.setPOJO(boolean pojo) {
//		if (this.inPOJOMode == pojo)
//			return;
//		PropertyManagement pm = PropertyManagement.getInstance();
//		Map<PersistingElement, Map<Field, Object>> toBeSet = new HashMap<PersistingElement, Map<Field,Object>>();
//		this.grabSetPOJOAtts(pojo, toBeSet);
//		for (Entry<PersistingElement, Map<Field, Object>> pe : toBeSet.entrySet()) {
//			synchronized (pe.getKey()) {
//				for (Entry<Field, Object> prop : pe.getValue().entrySet()) {
//					if (pojo == false) {
//						this.getColumnFamily(prop.getKey().getName()).updateFromPOJO();
//					}
//					pm .candideSetValue(pe.getKey(), prop.getKey(), prop.getValue());
//				}
//				pe.getKey().inPOJOMode = true;
//			}
//		}
	}
		
//	private void PersistingElement.grabSetPOJOAtts(boolean pojo, Map<PersistingElement, Map<Field, Object>> toBeSet) {
//		if (this.inPOJOMode == pojo)
//			return;
//		Map<Field, Object> thisToBeSet = new HashMap<Field, Object>();
//		PropertyManagement pm = PropertyManagement.getInstance();
//		for (Field f : pm.getProperties(this.getClass())) {
//			if (PersistingElement.class.isAssignableFrom(f.getType()))
//				((PersistingElement)pm.candideReadValue(this, f)).grabSetPOJOAtts(pojo, toBeSet);
//		}
//		ColumnFamiliyManagement cfm = ColumnFamiliyManagement.getInstance();
//		for (Field f : cfm.detectColumnFamilies(this.getClass())) {
//			ColumnFamily<?> cf = this.getColumnFamiliesInt().get(f.getName());
//			if (cf == null) {
//				cf = cfm.createColumnFamily(this, f, pm.candideReadValue(this, f));
//			}
//			if (cf.getClazz().isAssignableFrom(PersistingElement.class) || PersistingElement.class.isAssignableFrom(cf.getClazz())) {
//				for (String k : cf.getKeys()) {
//					Object o = cf.getElement(k);
//					if (o instanceof PersistingElement) {
//						((PersistingElement)o).grabSetPOJOAtts(pojo, toBeSet);
//					}
//				}
//			}
//			Object val = pojo ? cf.getSerializableVersion() : cf;
//			if (f.getType().isInstance(val)) {
//				thisToBeSet.put(f, val);
//			} else {
//				throw new ClassCastException("Cannot set " + f + " to " + val + " in " + this);
//			}
//		}
//		toBeSet.put(this, thisToBeSet);
//	}
	
	public void PersistingElement.updateFromPOJO() {
		for (ColumnFamily<?> cf : this.getColumnFamiliesInt().values()) {
			cf.updateFromPOJO();
		}
	}
	
	void around(PersistingElement self, Object cf): set(!@Transient !transient !static (Set+ || Map+) PersistingElement+.*) && target(self) && args(cf) {

		FieldSignature sign = (FieldSignature)thisJoinPointStaticPart.getSignature();
		Field field = sign.getField();
		assert isCollectionFamily(field);

		ColumnFamily<?> ccf = createColumnFamily(self, field, cf);
		
		if(ColumnFamily.class.isAssignableFrom(field.getType()))
			proceed(self, ccf);
		else if (cf == null)
			proceed(self, ccf.getSerializableVersion());
		else
			proceed(self, cf);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private ColumnFamily<?> createColumnFamily(PersistingElement self, Field field, Object oldCf) {
		PropertyManagement pm = PropertyManagement.getInstance();
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
	
	after(ColumnFamily cf) returning : execution(ColumnFamily.new(..)) && target(cf) {
		cf.getOwner().addColumnFamily(cf);
	}
}
