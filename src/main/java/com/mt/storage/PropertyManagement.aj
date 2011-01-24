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

import com.mt.storage.conversion.ConversionTools;

public aspect PropertyManagement {

	declare soft : DatabaseNotReachedException : within(PropertyManagement) && adviceexecution();

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
						&& PropertyManagement.aspectOf().isPropertyType(this.getField().getType())
						&& !this.getField().isAnnotationPresent(ExplicitActivation.class)) {
					Object val = this.getValue();
					if (val instanceof PersistingElement)
						((PersistingElement) this.getValue()).activateSimpleProperties();
				}
				this.wasActivated = true;
			}
		}

	}

	/**
	 * Stores non-transient non-null properties for persisting objects.
	 * 
	 */
	public static class PropertyFamily extends ColumnFamily<Property> {

		private PropertyFamily(PersistingElement owner)
				throws SecurityException, NoSuchFieldException {
			super(Property.class, null, PROPERTY_COLUMNFAMILY_NAME, owner,
					Property.class.getDeclaredField("name"), false, false);
		}

		@Override
		protected Property convert(String key, byte[] rep) {
			return new Property(key, rep);
		}
		
		protected synchronized void activate() throws DatabaseNotReachedException {
			super.activate(null);
			this.getOwner().upgradeProperties();
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
				if ((f.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) != 0
						|| Collection.class.isAssignableFrom(f.getType()))
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
			if (!KeyManagement.aspectOf().isKey(prop))
				return false;
			return true;
		}
		return false;
	}

	public boolean isSimplePropertyType(Class<?> type) {
		if (type.isEnum()) {
			return true;
		}
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

	public void PersistingElement.activateSimpleProperties() throws DatabaseNotReachedException {
		PropertyFamily pf = this.getProperties();
		if (pf != null) {
			pf.activate();
		}
	}
	
	private transient Map<Field, byte []> PersistingElement.lastState = new HashMap<Field, byte []>();
	
	void PersistingElement.storeProperties() {
		for (Field f : PropertyManagement.aspectOf().getProperties(this.getClass())) {
			if (f.isAnnotationPresent(Incrementing.class))
				continue;
			Object val = PropertyManagement.aspectOf().candideReadValue(this, f);
			if (val == null) {
				if (!this.lastState.containsKey(f) || this.lastState.get(f) != null) {
					this.getProperties().removeKey(f.getName());
					this.lastState.put(f, null);
				}
			} else {
				byte [] valB = ConversionTools.convert(val);
				if (!this.lastState.containsKey(f) || !Arrays.equals(valB, this.lastState.get(f))) {
					this.getProperties().add(new Property(f, val));
					this.lastState.put(f, valB);
				}
			}
		}
	}
	
	void PersistingElement.upgradeProperties() throws DatabaseNotReachedException {
		for (Field f : PropertyManagement.aspectOf().getProperties(this.getClass())) {
			if (f.isAnnotationPresent(Incrementing.class))
				continue;
			Object oldVal = null;
			boolean oldValRead = false;
			try {
				oldVal = PropertyManagement.aspectOf().readValue(this, f);
				oldValRead = true;
			} catch (Exception x) {
			}
			
			Object val;
			
			Property prop = (Property) this.getProperties().get(f.getName());
			if (prop != null) {
				val = prop.getValue();
				if (oldVal != null && val != null && (oldVal instanceof PersistingElement) && (val instanceof byte []) && ConversionTools.convert(String.class, (byte[])val).equals(((PersistingElement)oldVal).getIdentifier())) {
					prop.setValue(oldVal);
				}
				if (prop.getField() == null)
					prop.setField(f);
				assert prop.getField().equals(f);
				if (this.getProperties().wasActivated())
					prop.activate();
				val = prop.getValue();
			} else {
				val = null;
			}
			
			if (PersistingElement.class.isAssignableFrom(f.getType()) && val != null && oldVal != null && val != oldVal && ((PersistingElement)val).getIdentifier().equals(((PersistingElement)oldVal).getIdentifier())) {
				prop.setValue(oldVal);
				prop.setField(f);
				if (this.getProperties().wasActivated())
					prop.reactivate();
				continue;
			}

			byte [] oldValB = oldVal == null ? null : ConversionTools.convert(oldVal);
			byte [] valB = val == null ? null : ConversionTools.convert(val);
	
			if (oldValRead && (oldVal == null ? val == null : Arrays.equals(oldValB, valB))) {
				this.lastState.put(f, valB);
				continue;
			}
	
			if ((f.getModifiers() & Modifier.FINAL) == 0) {
				// Setting proper value in property
				try {
					PropertyManagement.aspectOf().setValue(this, f, val);
					this.lastState.put(f, valB);
				} catch (Exception x) { //May happen in case f is of simple type (e.g. boolean and not Boolean) and value is unknown from the base (i.e. null)
					this.lastState.put(f, oldValB);
				}
			}
		}
	}

	pointcut attUpdated(PersistingElement self, Object val): set(!transient !static (!Collection+) PersistingElement+.*) && target(self) && args(val);

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
