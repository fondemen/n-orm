package com.mt.storage;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mt.storage.conversion.ConversionTools;

public aspect KeyManagement {
	private static KeyManagement INSTANCE;
	
	public static KeyManagement getInstance() {
		if (INSTANCE == null)
			INSTANCE = aspectOf();
		return INSTANCE;
	}

	declare error: set(@Key double PersistingElement+.*) : "Floating values not supported in keys...";
	declare error: set(@Key java.lang.Double PersistingElement+.*) : "Floating values not supported in keys...";
	declare error: set(@Key float PersistingElement+.*) : "Floating values not supported in keys...";
	declare error: set(@Key java.lang.Float PersistingElement+.*) : "Floating values not supported in keys...";
	
	private Map<Class<?>, List<Field>> typeKeys = new HashMap<Class<?>, List<Field>>();

	private transient String PersistingElement.identifier;
	private transient String PersistingElement.fullIdentifier;
	
	after(PersistingElement self) returning: PersistingMixin.creation(self) {
		self.identifier = this.createIdentifier(self, self.getClass());
	}
	
	@SuppressWarnings("unchecked")
	public <T> T createElement(Class<T> expectedType, String id) {
		Class<? extends T> actualType = expectedType;
		{//Detecting whether this identifier contains the class of the element to be created
			int clsSep = id.lastIndexOf(StorageManagement.INDENTIFIER_CLASS_SEPARATOR);
			if (clsSep > 0) {
				String className = id.substring(clsSep + StorageManagement.INDENTIFIER_CLASS_SEPARATOR.length());
				if (!className.contains(this.getSeparator(expectedType))) {
					Class<?> detectedClass;
					try {
						detectedClass = Class.forName(className);
					} catch (ClassNotFoundException e) {
						throw new IllegalArgumentException("Cannot find class " + className + " as designated by id " + id, e);
					}
					if (! expectedType.isAssignableFrom(detectedClass)) {
						throw new IllegalArgumentException("Cannot create a " + expectedType + " with an id designating a non-compatible class " + detectedClass + " (id is " + id + ')');
					}
					actualType = (Class <? extends T>)detectedClass;
					id = id.substring(0, clsSep);
				}
			}
		}
		
		if(!canCreateFromKeys(actualType))
			throw new IllegalArgumentException("Non-persisting " + actualType + " should have either no or only properties annotated with " + Key.class);
		
		try {
			List<Field> tkeys = typeKeys.get(actualType);
			if (tkeys == null) {
				this.detectKeys(actualType);
				tkeys = typeKeys.get(actualType);
			}
			Class<?>[] tkeyTypes = new Class<?>[tkeys.size()];
			int i = 0;
			for (Field key : tkeys) {
				tkeyTypes[i] = key.getType();
				i++;
			}
			Constructor<? extends T> constr = actualType.getConstructor(tkeyTypes);
			
			String separator = this.getSeparator(actualType);
			Object[] keyValues = ConversionTools.decompose(id, separator, tkeyTypes);
			
			return constr.newInstance(keyValues);
		} catch (Exception x) {
			throw new RuntimeException("Cannot build new instance of " + actualType + " with key " + id, x);
		}
	}

	public boolean canCreateFromKeys(Class<?> type) {
		if (!PersistingElement.class.isAssignableFrom(type)) {
			for (Field property : PropertyManagement.getInstance().getProperties(type)) {
				if (PropertyManagement.getInstance().isProperty(property) && ! this.isKey(property))
					return false;
			}
		}
		return true;
	}

	public <T> String getSeparator(Class<T> clazz) {
		return ":";
	}
	
	public List<Field> PersistingElement.getKeys() {
		return new ArrayList<Field>(KeyManagement.getInstance().typeKeys.get(this.getClass()));
	}

	public List<Field> detectKeys(Class<?> clazz) {
		if (! typeKeys.containsKey(clazz)) {
			if (PropertyManagement.getInstance().isSimplePropertyType(clazz))
				return new ArrayList<Field>(0);
			ArrayList<Field> foundKeys = new ArrayList<Field>();
			int maxKeyOrder = -1;
			boolean isPersisting = PersistingElement.class.isAssignableFrom(clazz);
			
			for (Field f : PropertyManagement.getInstance().getProperties(clazz)) {
				if (f.isAnnotationPresent(Key.class)) {
					this.checkKey(f);
					
					Key keyDescriptor = f.getAnnotation(Key.class);
					int order = keyDescriptor.order()-1;
					while (foundKeys.size() <= order) foundKeys.add(null);
					Field old = foundKeys.set(order, f);
					if (old != null)
						throw new IllegalArgumentException("Class " + clazz + " has keys with same order " + f + " and " + old);
					maxKeyOrder = Math.max(maxKeyOrder, order);
				} else {
					if (isPersisting)
						PropertyManagement.getInstance().checkProperty(f);
					else
						throw new IllegalStateException("The non persisting " + clazz + " should have either only or no property annotated with " + Key.class + " unlike property " + f );
				}
			}
			
			if (maxKeyOrder < 0)
				throw new IllegalStateException("A persisting class should declare at least a field annotated with " + Key.class + " unlike " + clazz);
			
			for (int i = 1; i < maxKeyOrder; i++) {
				if (foundKeys.get(i) == null)
					throw new IllegalArgumentException("Class " + clazz + " misses key of index " + i + " ; check that your key are all public.");
			}
			
			foundKeys.trimToSize();
			
			typeKeys.put(clazz, foundKeys);
		}
		return typeKeys.get(clazz);
	}
	
	public void checkKey(Field f) {
		PropertyManagement.getInstance().checkProperty(f);
		
		if (f.getAnnotation(Key.class).order() <= 0)
			throw new IllegalArgumentException("A key should declare an order which must bhe at least 1.");
		
		if ((f.getModifiers()&Modifier.FINAL) == 0)
			throw new IllegalStateException("The key " + f + " should be final.");
	}
	
	public boolean isKey(Field key) {
		try {
			this.checkKey(key);
			return true;
		} catch (Exception x) {
			return false;
		}
	}
	
	public String createIdentifier(Object element, Class<?> expected) {
		if (! expected.isInstance(element))
			throw new ClassCastException("Element " + element + " of class " + element.getClass() + " is not compatible with " + expected);
		try {
			StringBuffer ret = new StringBuffer();
			
			if ((element instanceof PersistingElement) && ((PersistingElement)element).identifier != null) {
				ret.append(((PersistingElement)element).identifier);
			} else {
				String sep = this.getSeparator(element.getClass());
				boolean fst = true;
				for (Field key : this.detectKeys(element.getClass())) {
					if (fst) fst = false; else ret.append(sep);
					Object o = PropertyManagement.getInstance().readValue(element, key);
					if (o == null)
						throw new IllegalStateException("A key cannot be null.");
					ret.append(ConversionTools.convertToString(o, key.getType()));
				}
			}
			
			if (!element.getClass().equals(expected)) {
				ret.append(StorageManagement.INDENTIFIER_CLASS_SEPARATOR);
				ret.append(element.getClass().getName());
			}
			
			return ret.toString();
		} catch (RuntimeException x) {
			throw x;
		} catch (Exception x) {
			throw new RuntimeException(x);
		}
	}


	public String PersistingElement.getIdentifier() {
		return this.identifier;
	}


	public String PersistingElement.getFullIdentifier() {
		if (this.fullIdentifier == null)
			this.fullIdentifier = KeyManagement.getInstance().createIdentifier(this, PersistingElement.class);
		return this.fullIdentifier;
	}
}
