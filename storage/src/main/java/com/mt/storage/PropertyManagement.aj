package com.mt.storage;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.beanutils.PropertyUtils;
import org.aspectj.lang.SoftException;

import com.mt.storage.cf.ColumnFamily;
import com.mt.storage.cf.MapColumnFamily;
import com.mt.storage.conversion.ConversionTools;


public aspect PropertyManagement {
	private static PropertyManagement INSTANCE;
	
	public static PropertyManagement getInstance() {
		if (INSTANCE == null)
			INSTANCE = aspectOf();
		return INSTANCE;
	}

	declare soft : DatabaseNotReachedException : within(PropertyManagement) && adviceexecution();
	
//	declare error: set(!static !transient (!Collection+ && !java.io.Serializable+) PersistingElement+.*) : "Non serializable field ; may break element's serialization";

	declare warning: get(@ExplicitActivation transient * PersistingElement+.*)
		|| get(@ExplicitActivation static * PersistingElement+.*) : "This field is not persitent, thus cannot be auto-activated";

	public static final String PROPERTY_COLUMNFAMILY_NAME = "props";
	public static final int MAXIMUM_PROPERTY_NUMBER = 256;

	public static class Property {
		private final String name;
		private Field field;
		private Object value;
		private boolean wasActivated = false;

		private Property(String fieldName, Object value) {
			super();
			this.setValue(value);
			this.name = fieldName;
			
		}

		private Property(Field field, Object value) {
			this(field.getName(), value);
			this.setField(field);
		}

		public Object getValue() {
			assert this.value != null;
			return value;
		}

		public void setValue(Object value) {
			if (value == null)
				throw new NullPointerException();
			this.value = value;
		}

		public String getName() {
			return name;
		}

		public Field getField() {
			return field;
		}

		void setField(Field field) {
			this.field = field;
			this.setType(field.getType());
		}

		@Override
		public int hashCode() {
			return this.getName().hashCode();
		}

		@Override
		public String toString() {
			return this.getValue().toString();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Property)
				obj = ((Property) obj).getValue();
			if (this.getValue() instanceof byte[] && !(obj instanceof byte[])) {
				try {
					return ConversionTools.convert(obj.getClass(),
							(byte[]) this.getValue()).equals(obj);
				} catch (Exception x) {
				}
			}
			return getValue().equals(obj);
		}

		private void setType(Class<?> type) {
			if (this.getValue() instanceof byte[]) {
				// In case it was read from an activation, it's not of the
				// proper type
				if (!byte[].class.equals(type)) {
					this.setValue(ConversionTools.convert(type,
							((byte[]) this.getValue())));
				}
			}
		}

		public void activate() throws DatabaseNotReachedException {
			if (this.wasActivated)
				return;
			reactivate();
		}

		private void reactivate() throws DatabaseNotReachedException {
			if (this.getField() != null) {
				if (this.getValue() != null
						&& PropertyManagement.getInstance().isPropertyType(this.getField().getType())
						&& !this.getField().isAnnotationPresent(ExplicitActivation.class)) {
					Object val = this.getValue();
					if (val instanceof PersistingElement)
						((PersistingElement) this.getValue()).activate();
				}
				this.wasActivated = true;
			}
		}

	}

	/**
	 * Stores non-transient non-null properties for persisting objects.
	 * 
	 */
	public static class PropertyFamily extends MapColumnFamily<String, Property> {

		private PropertyFamily(PersistingElement owner)
				throws SecurityException, NoSuchFieldException {
			super(String.class, Property.class, null, PROPERTY_COLUMNFAMILY_NAME, owner, false, false);
		}

		@Override
		protected Property preparePut(String key, byte[] rep) {
			return new Property(key, rep);
		}

		@Override
		protected boolean hasChanged(String key, Property lhs, Property rhs) {
			if(lhs == rhs)
				return false;
			
			assert lhs.getField() == rhs.getField();
			
			Class<?> clazz = lhs.getField().getType();
			
			return Arrays.equals(ConversionTools.convert(lhs, clazz), ConversionTools.convert(rhs, clazz));
		}

	}

	private Map<Class<?>, Set<Field>> typeProperties = new HashMap<Class<?>, Set<Field>>();

	public Set<Field> getProperties(Class<?> type) {
		if (!this.typeProperties.containsKey(type)) {
			Set<Field> ret = new HashSet<Field>(Arrays.asList(type
					.getDeclaredFields()));
			Class<?> supertype = type.getSuperclass();
			if (supertype != null)
				ret.addAll(this.getProperties(supertype));
			for (Field f : new ArrayList<Field>(ret)) {
				Class<?> ft = f.getType();
				if ((f.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) != 0
						|| Collection.class.isAssignableFrom(ft)
						|| Map.class.isAssignableFrom(ft)
						|| ColumnFamily.class.isAssignableFrom(ft))
					ret.remove(f);
			}
			this.typeProperties.put(type, ret);
		}
		return this.typeProperties.get(type);
	}
	
	public Field getProperty(Class<?> type, String name) {
		for (Field p : this.getProperties(type)) {
			if (p.getName().equals(name))
				return p;
		}
		return null;
	}

	public void checkProperty(Field f) {
		if ((f.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) != 0)
			return;
		
		if ((f.getModifiers()&Modifier.FINAL) != 0)
			throw new IllegalStateException("The property " + f + " should not be final.");

		Class<?> type = f.getType();
		if (type.isArray())
			type = type.getComponentType();
		if (!this.isPropertyType(type))
			throw new IllegalStateException(
					"The "
							+ f
							+ " key should be of a type that must be a primitive, a String, an enumeration, a class annotated with "
							+ Persisting.class
							+ ", a class whose properties are all annotated with "
							+ Key.class
							+ " or an array of such types.");
	}

	public boolean isProperty(Field prop) {
		try {
			this.checkProperty(prop);
			return true;
		} catch (Exception x) {
			return false;
		}
	}

	public boolean isPropertyType(Class<?> type) {
		return isPersistingPropertyType(type)
				|| this.isNonPersistingPropertyType(type);
	}

	public boolean isPersistingPropertyType(Class<?> type) {
		return PersistingElement.class.isAssignableFrom(type);
	}

	public boolean isNonPersistingPropertyType(Class<?> type) {
		if (this.isSimplePropertyType(type))
			return true;
		for (Field prop : this.getProperties(type)) {
			if (!KeyManagement.getInstance().isKey(prop))
				return false;
			return true;
		}
		return false;
	}

	public boolean isSimplePropertyType(Class<?> type) {
		if (type.isEnum()) {
			return true;
		}
		if (type.isArray())
			type = type.getComponentType();
		for (Class<?> possibleSupertype : PersistingElement.PossiblePropertyTypes) {
			if (possibleSupertype.isAssignableFrom(type))
				return true;
		}
		return false;
	}

	private transient PropertyFamily PersistingElement.properties;

	/**
	 * The column family used to store properties.
	 */
	public PropertyFamily PersistingElement.getProperties() {
		if (this.properties == null)
			try {
				this.properties = new PropertyFamily(this);
			} catch (RuntimeException x) {
				throw x;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		return this.properties;
	}
	
	private transient Map<Field, byte []> PersistingElement.lastState = new HashMap<Field, byte []>();
	
	void PersistingElement.storeProperties() {
		PropertyManagement pm = PropertyManagement.getInstance();
		KeyManagement km = KeyManagement.getInstance();
		for (Field f : pm.getProperties(this.getClass())) {
			if (km.isKey(f))
				continue;
			if (f.isAnnotationPresent(Incrementing.class))
				continue;
			Object val = pm.candideReadValue(this, f);
			if (val == null) {
				if (!this.lastState.containsKey(f) || this.lastState.get(f) != null) {
					this.getProperties().removeKey(f.getName());
					this.lastState.put(f, null);
				}
			} else {
				byte [] valB = ConversionTools.convert(val, f.getType());
				if (!this.lastState.containsKey(f) || !Arrays.equals(valB, this.lastState.get(f))) {
					this.getProperties().put(f.getName(), new Property(f, val));
					this.lastState.put(f, valB);
				}
			}
		}
	}
	
	void PersistingElement.upgradeProperties() throws DatabaseNotReachedException {
		PropertyManagement pm = PropertyManagement.getInstance();
		KeyManagement km = KeyManagement.getInstance();
		PropertyFamily props = this.getProperties();
		for (Field f : pm.getProperties(this.getClass())) {
			if (f.isAnnotationPresent(Incrementing.class))
				continue;
			Object oldVal = null;
			boolean oldValRead = false;
			try {
				oldVal = pm.readValue(this, f);
				oldValRead = true;
			} catch (Exception x) {
			}
			
			Object val;
			
			Property prop = (Property) props.getElement(f.getName());
			if (prop != null) {
				val = prop.getValue();
				if (oldVal != null && val != null && (oldVal instanceof PersistingElement) && (val instanceof byte []) && ConversionTools.convert(String.class, (byte[])val).equals(((PersistingElement)oldVal).getIdentifier())) {
					prop.setValue(oldVal);
				}
				if (prop.getField() == null)
					prop.setField(f);
				assert prop.getField().equals(f);
				prop.activate();
				val = prop.getValue();
			} else {
				val = null;
			}
			
			if (PersistingElement.class.isAssignableFrom(f.getType()) && val != null && oldVal != null && val != oldVal && ((PersistingElement)val).getIdentifier().equals(((PersistingElement)oldVal).getIdentifier())) {
				prop.setValue(oldVal);
				prop.setField(f);
				prop.reactivate();
				continue;
			}

			byte [] oldValB = oldVal == null ? null : ConversionTools.convert(oldVal, f.getType());
			byte [] valB = val == null ? null : ConversionTools.convert(val, f.getType());
	
			if (oldValRead && (oldVal == null ? val == null : Arrays.equals(oldValB, valB))) {
				this.lastState.put(f, valB);
				continue;
			}
	
			if (! km.isKey(f)) {
				// Setting proper value in property
				try {
					pm.setValue(this, f, val);
					this.lastState.put(f, valB);
				} catch (Exception x) { //May happen in case f is of simple type (e.g. boolean and not Boolean) and value is unknown from the base (i.e. null)
					this.lastState.put(f, oldValB);
				}
			}
		}
	}

	pointcut attUpdated(PersistingElement self, Object val): set(!transient !static !(Collection+ || Map+ || ColumnFamily+) PersistingElement+.*) && target(self) && args(val);

	public Object candideReadValue(Object self, Field property) {
		try {
			return this.readValue(self, property);
		} catch (RuntimeException x) {
			throw x;
		} catch (Exception x) {
			throw new RuntimeException(x);
		}
	}

	public void candideSetValue(Object self, Field property, Object value) {
		try {
			this.setValue(self, property, value);
		} catch (RuntimeException x) {
			throw x;
		} catch (Exception x) {
			throw new RuntimeException(x);
		}
	}

	public Object readValue(Object self, Field property)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {
		try {
			return property.get(self);
		} catch (Exception x) {
			return PropertyUtils.getProperty(self, property.getName());
		}
	}

	public void setValue(Object self, Field property, Object value)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {
		try {
			property.set(self, value);
		} catch (Exception x) {
			try {
				PropertyUtils.setProperty(self, property.getName(), value);
			} catch (Exception y) {
				throw new SoftException(x);
			}
		}
	}
	
	before(Field f) : (call(Object Field.get(Object)) || call(void Field.set(Object, Object))) && target(f) && within(PropertyManagement) {
		try {
			f.setAccessible(true);
		} catch (SecurityException x) {}
	}

}
