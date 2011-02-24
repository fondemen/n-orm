package com.mt.storage;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.mt.storage.conversion.ArrayConverter;
import com.mt.storage.conversion.ConversionTools;

public aspect KeyManagement {
	public static final String KEY_SEPARATOR = ":"; 
	public static final String INDENTIFIER_CLASS_SEPARATOR = "`";
	
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
	
	private static class DecomposableString {
		private static final String keySeparator;
		private static final String arraySeparator;
		private static final String arrayEndSeparator;
		private static final String classSeparator;
		private static final Set<String> specialChars;
		
		static {
			keySeparator = KEY_SEPARATOR;
			arraySeparator = ArrayConverter.StringSeparator;
			arrayEndSeparator = ArrayConverter.StringEndSeparator;
			classSeparator = INDENTIFIER_CLASS_SEPARATOR;
			
			specialChars = new TreeSet<String>();
			specialChars.add(keySeparator);
			specialChars.add(arraySeparator);
			specialChars.add(arrayEndSeparator);
			specialChars.add(classSeparator);
		}
		
		private final KeyManagement km = KeyManagement.getInstance();
		private final String ident;
		private String rest;
		
		public DecomposableString(String ident) {
			this.ident = ident;
			this.rest = ident;
		}
		
		public boolean isEmpty() {
			return this.rest.isEmpty();
		}
		
		public void detect(String start) {
			if (this.rest.startsWith(start)) {
				this.rest = this.rest.substring(start.length());
			} else
				throw new IllegalArgumentException("Expecting " + start + " before " + this.rest + " on identifier " + this.ident);
		}
		
		public <U> U detect(Class<U> type) {
			if (type.isArray()) {
				return detectArray(type);
			} else {
				if (km.detectKeys(type).size() > 0) {
					return detectKeyedElement(type);
				} else {
					return detectSimpleElement(type);
				}
			}
		}

		private <U> U detectSimpleElement(Class<U> type) {
			String ret = new String(this.rest);
			for (String sc : specialChars) {
				int i = ret.indexOf(sc);
				if (i >= 0)
					ret = ret.substring(0, i);
			}
			this.detect(ret);
			return ConversionTools.convertFromString(type, ret);
		}

		@SuppressWarnings("unchecked")
		private <U> U detectKeyedElement(Class<U> type) {
			List<Field> keys = km.detectKeys(type);
			Object [] vals = new Object[keys.size()];
			for (int i = 0; i < vals.length; i++) {
				if (i != 0)
					this.detect(keySeparator);
				vals[i] = this.detect(keys.get(i).getType());
			}
			Class<? extends U> actualType = type;
			if (this.rest.startsWith(classSeparator)) {
				this.detect(classSeparator);
				String clsName = this.detectSimpleElement(String.class);
				try {
					actualType = (Class<? extends U>) Class.forName(clsName);
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException("Cannot find class " + clsName + " as declared in key " + this.ident + " before " + this.rest, e);
				}
			}
			return KeyManagement.getInstance().createElement(actualType, vals);
		}

		private <U> U detectArray(Class<U> type) {
			Class<?> componentType = type.getComponentType();
			List<Object> res = new LinkedList<Object>();
			boolean fst = true;
			while (! rest.startsWith(arrayEndSeparator) && ! rest.isEmpty()) {
				if (fst) {
					fst = false;
					assert ! this.rest.startsWith(arraySeparator);
				} else {
					this.detect(arraySeparator);
				}
				res.add(this.detect(componentType));
			}
			if (! this.rest.isEmpty())
				this.detect(arrayEndSeparator);
			@SuppressWarnings("unchecked")
			U ret = (U) Array.newInstance(type.getComponentType(), res.size());
			int i = 0;
			for (Object elt : res) {
				Array.set(ret, i, elt);
				i++;
			}
			return ret;
		}
	}
	
	private Map<Class<?>, List<Field>> typeKeys = new HashMap<Class<?>, List<Field>>();

	private transient String PersistingElement.identifier;
	private transient String PersistingElement.fullIdentifier;
	
	after(PersistingElement self) returning: PersistingMixin.creation(self) {
		self.identifier = this.createIdentifier(self, self.getClass());
	}
	
	public <T> T createElement(Class<T> expectedType, String id) {
		return new DecomposableString(id).detect(expectedType);
	}
	
	@SuppressWarnings("unchecked")
	private <T> T createElement(Class<T> type, Object [] keyValues) {
		
		if(!canCreateFromKeys(type))
			throw new IllegalArgumentException("Non-persisting " + type + " should have either no or only properties annotated with " + Key.class);
		
		try {
			List<Field> tkeys = typeKeys.get(type);
			if (tkeys == null) {
				this.detectKeys(type);
				tkeys = typeKeys.get(type);
			}
			Class<?>[] tkeyTypes = new Class<?>[tkeys.size()];
			int i = 0;
			for (Field key : tkeys) {
				tkeyTypes[i] = key.getType();
				i++;
			}
			Constructor<? extends T> constr = type.getConstructor(tkeyTypes);
			
			return constr.newInstance(keyValues);
		} catch (Exception x) {
			String[] strVals = new String[keyValues.length];
			for (int i = 0; i < strVals.length; i++) {
				strVals[i] = ConversionTools.convertToString(keyValues[i]);
			}
			throw new RuntimeException("Cannot build new instance of " + type + " with key values " + strVals, x);
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
				boolean fst = true;
				for (Field key : this.detectKeys(element.getClass())) {
					if (fst) fst = false; else ret.append(KEY_SEPARATOR);
					Object o = PropertyManagement.getInstance().readValue(element, key);
					if (o == null)
						throw new IllegalStateException("A key cannot be null.");
					ret.append(ConversionTools.convertToString(o, key.getType()));
				}
			}
			
			if (!element.getClass().equals(expected)) {
				ret.append(INDENTIFIER_CLASS_SEPARATOR);
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
