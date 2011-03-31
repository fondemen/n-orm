package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.beanutils.PropertyUtils;
import org.aspectj.lang.SoftException;


import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.ExplicitActivation;
import com.googlecode.n_orm.Incrementing;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.cf.MapColumnFamily;
import com.googlecode.n_orm.conversion.ConversionTools;


public aspect PropertyManagement {
	private static PropertyManagement INSTANCE;
	
	public static PropertyManagement getInstance() {
		if (INSTANCE == null)
			INSTANCE = aspectOf();
		return INSTANCE;
	}
	
//	declare error: set(!@Transient !static !transient (!Collection+ && !java.io.Serializable+) PersistingElement+.*) : "Non serializable field ; may break element's serialization";

	declare warning: get(@ExplicitActivation transient * PersistingElement+.*)
		|| get(@Transient @ExplicitActivation static * PersistingElement+.*)
		|| get(@ExplicitActivation static * PersistingElement+.*)
		: "This field is not persitent, thus cannot be auto-activated";
	declare warning: set(@Transient transient * PersistingElement+.*) : "There is no need to annotate a transient field with @Transient";

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
						((PersistingElement) this.getValue()).activateIfNotAlready();
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
		
		private transient Map<Field, byte []> lastState = new HashMap<Field, byte []>();

		private PropertyFamily(PersistingElement owner)
				throws SecurityException, NoSuchFieldException {
			super(String.class, Property.class, null, PROPERTY_COLUMNFAMILY_NAME, owner, false);
			assert this.changes != null;
			List<Field> keysF = KeyManagement.getInstance().detectKeys(getOwner().getClass());
			final Set<String> keys = new TreeSet<String>();
			for (Field key : keysF) {
				keys.add(key.getName());
			}
			this.changes = new TreeMap<String, ChangeKind>() {
				private static final long serialVersionUID = 1L;

				@Override
				public ChangeKind put(String key, ChangeKind value) {
					if (keys.contains(key))
						return null;
					return super.put(key, value);
				}
				
			};
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
			
			return !Arrays.equals(ConversionTools.convert(lhs, clazz), ConversionTools.convert(rhs, clazz));
		}

		@Override
		public void updateFromPOJO() {
			PropertyManagement pm = PropertyManagement.getInstance();
			KeyManagement km = KeyManagement.getInstance();
			PersistingElement owner = this.getOwner();
			for (Field f : pm.getProperties(owner.getClass())) {
				if (f.isAnnotationPresent(Incrementing.class))
					continue;
				Object val = pm.candideReadValue(owner, f);
				if (val == null) {
					if (km.isKey(f))
						throw new IllegalStateException("Key " + f + " is left null for " + owner);
					if (!this.lastState.containsKey(f) || this.lastState.get(f) != null) {
						this.removeKey(f.getName());
						this.lastState.put(f, null);
					}
				} else {
					byte [] valB = ConversionTools.convert(val, f.getType());
					if (!this.lastState.containsKey(f) || !Arrays.equals(valB, this.lastState.get(f))) {
						this.put(f.getName(), new Property(f, val));
						this.lastState.put(f, valB);
					}
				}
			}
		}

		@Override
		public void storeToPOJO() {
			PropertyManagement pm = PropertyManagement.getInstance();
			KeyManagement km = KeyManagement.getInstance();
			PersistingElement owner = this.getOwner();
			for (Field f : pm.getProperties(owner.getClass())) {
				if (f.isAnnotationPresent(Incrementing.class))
					continue;
				Object oldVal = null;
				boolean oldValRead = false;
				try {
					oldVal = pm.readValue(owner, f);
					oldValRead = true;
				} catch (Exception x) {
				}
				
				Object val;
				
				Property prop = (Property) this.getElement(f.getName());
				if (prop == null && km.isKey(f)) { //Keys might not be in properties just after activation
					prop = new Property(f, oldVal);
					this.put(f.getName(), prop);
				}
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
						pm.setValue(owner, f, val);
						this.lastState.put(f, valB);
					} catch (Exception x) { //May happen in case f is of simple type (e.g. boolean and not Boolean) and value is unknown from the base (i.e. null)
						this.lastState.put(f, oldValB);
					}
				} else
					this.lastState.put(f, oldValB);
			}
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
						|| f.isAnnotationPresent(Transient.class)
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
		if (f.isAnnotationPresent(Transient.class))
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
	public PropertyFamily PersistingElement.getPropertiesColumnFamily() {
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

	pointcut attUpdated(PersistingElement self, Object val): set(!@Transient !transient !static !(Collection+ || Map+ || ColumnFamily+) PersistingElement+.*) && target(self) && args(val);

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
