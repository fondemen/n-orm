package com.googlecode.n_orm;

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

import org.aspectj.lang.reflect.FieldSignature;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.conversion.ArrayConverter;
import com.googlecode.n_orm.conversion.ConversionTools;



/**
 * To be identifiable, a persisting object must define fields to setup the key by annotating them with {@link Key}.
 * Keys must declare an order from 1 to N, must not overlap, and must be all there, e.g. if N=3, the class must declare keys for orders 1, 2 and 3.
 * Keys are inherited.
 * For non-persisting elements (i.e. elements that are not annotated with {@link Persisting}, all fields must be keys.
 * As such, an element may be identified by a string, whose format is the following:
 * <code>
 * id ::=   sid //for a simple element such as a character string, an integer, an enum...<br>
 *        | id ( {@link KeyManagement#KEY_SEPARATOR} id)* ({@link KeyManagement#KEY_END_SEPARATOR} sid)? '}' //for an element as decribed above ; each included id representing its key values, and the last optional sid representing its class<br>
 *        | id ( {@link ArrayConverter#StringSeparator}  id)* //for an array<br>
 * sid ::= (~({@link KeyManagement#KEY_SEPARATOR}|{@link KeyManagement#KEY_END_SEPARATOR}|{@link ArrayConverter#StringSeparator}))*
 * </code>
 * @author fondemen
 *
 */
public aspect KeyManagement {
	public static final String KEY_SEPARATOR = "\u0017";  //Shouldn't be a printable char
	public static final String KEY_END_SEPARATOR = "\u0001"; //As small as possible so that {v="AA"}.identifier (= "AA"+KEY_END_SEPARATOR) < {v="AAA"}.identifier (= "AAA"+KEY_END_SEPARATOR)
	
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
	//declare error: PersistingElement+ && hasField(@Key double *) : "Floating values not supported in keys..."; //AJ 1.6.9 style
	declare error: set(@Key final * *.*) : "A key should not be final";
	
	private static class DecomposableString {
		private static final String keySeparator;
		private static final String arraySeparator;
		private static final String keyEndSeparator;
		private static final String[] specialChars;
		
		static {
			keySeparator = KEY_SEPARATOR;
			arraySeparator = ArrayConverter.StringSeparator;
			keyEndSeparator = KEY_END_SEPARATOR;
			
			specialChars = new String [3];
			specialChars[0] = keySeparator;
			specialChars[1] = arraySeparator;
			specialChars[2] = keyEndSeparator;
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
		
		public <U> U detect(Class<U> type) {
			U ret = this.detectLast(type);
			if (! this.isEmpty()) {
				throw new IllegalArgumentException("Could not analyze the complete string: " + this.rest + " left over while analyzing " + this.ident + " as a " + type + " instance.");
			}
			return ret;
		}
		
		protected <U> U detectLast(Class<U> expected) {
			if (expected.isArray()) {
				return detectLastArray(expected);
			} else {
				if (km.detectKeys(expected).size() > 0) {
					return detectLastKeyedElement(expected);
				} else {
					return detectLastSimpleElement(expected);
				}
			}
		}
		
		private <U> U detectLastArray(Class<U> expected) {
			Class<?> componentType = expected.getComponentType();
			LinkedList<Object> elements = new LinkedList<Object>();
			String restSav = this.rest;
			do {
				try {
					elements.addFirst(this.detectLast(componentType));
					restSav = this.rest;
				} catch (Exception x) {
					this.rest = restSav;
					break;
				}
			} while (this.detectLast(arraySeparator) && !this.rest.isEmpty());
			
			Object[] res = elements.toArray();
			@SuppressWarnings("unchecked")
			U ret = (U) Array.newInstance(componentType, res.length);
			System.arraycopy(res, 0, ret, 0, res.length);
			return ret;
		}
		
		@SuppressWarnings("unchecked")
		private <U> U detectLastKeyedElement(Class<U> expected) {
			Class<? extends U> actualType = expected;
			String actualTypeName = this.detectLastUpTo(false, keyEndSeparator);
			if (!actualTypeName.isEmpty()) {
				try {
					Class<?> detectedCls = Class.forName(actualTypeName);
					actualType = (Class<? extends U>) detectedCls;
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException("Cannot find class " + actualTypeName + " as declared in key " + this.ident + " after " + this.rest, e);
				} catch (ClassCastException e) {
					throw new IllegalArgumentException("Expecting a " + actualTypeName + " element while identifier declares incompatible type " + actualTypeName + " (identifier is " + this.ident + ')', e);
				}
			}
			
			String ident = this.rest;
			this.detectLast(keyEndSeparator);
			List<Field> keys = km.detectKeys(actualType);
			Object [] vals = new Object[keys.size()];
			for (int i = vals.length-1; i >= 0; i--) {
				vals[i] = this.detectLast(keys.get(i).getType());
				if (i > 0)
					this.checkLast(keySeparator);
			}
			ident = ident.substring(this.rest.length());
			U ret = KeyManagement.getInstance().createElement(actualType, vals);
			if (ret instanceof PersistingElement) {
				((PersistingElement)ret).identifier = ident;
				((PersistingElement)ret).getFullIdentifier();
			}
			return ret;
		}
		
		private <U> U detectLastSimpleElement(Class<U> expected) {
			if (rest.endsWith(keyEndSeparator))
				throw new IllegalArgumentException("Detecting complex type at the end of " + this.rest + " while expecting element of simple type " + expected);
			return ConversionTools.convertFromString(expected, this.detectLastSimpleId());
		}
		
		protected String detectLastSimpleId() {
			return this.detectLastUpTo(false, specialChars);
		}
		
		protected void checkLast(String expected) {
			if (! this.detectLast(expected))
				throw new IllegalArgumentException("Expecting " + expected + " at the end of " + this.rest + " in identifier " + this.ident);
		}
		
		protected boolean detectLast(String expected) {
			if (expected.isEmpty())
				return true;
			if (! this.rest.endsWith(expected))
				return false;;
			this.rest = this.rest.substring(0, this.rest.length() - expected.length());
			return true;
		}
		
		protected String detectLastUpTo(boolean andIncluding, String... separators) {
			String ret = this.rest;
			String separator = null;
			for (String sc : separators) {
				int sep = ret.lastIndexOf(sc);
				if (sep >= 0) {
					ret = ret.substring(sep + sc.length());
					separator = sc;
				}
			}
			this.checkLast(ret);
			if (andIncluding && separator != null)
				this.checkLast(separator);
			return ret;
		}
	}
	
	private Map<Class<?>, List<Field>> typeKeys = new HashMap<Class<?>, List<Field>>();

	private transient String PersistingElement.identifier;
	private transient String PersistingElement.fullIdentifier;
	
	public <T> T createElement(Class<T> expectedType, String id) {
		try {
			return new DecomposableString(id).detect(expectedType);
		} catch (Exception x) {
			throw new IllegalArgumentException("Cannot create instance of " + expectedType + " with id " + id, x);
		}
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
			
			T ret;
			try { //using the default constructor
				ret = type.getConstructor().newInstance();
				i = 0;
				PropertyManagement pm = PropertyManagement.getInstance();
				for (Field key : tkeys) {
					pm.setValue(ret, key, keyValues[i]);
					i++;
				}
			} catch (NoSuchMethodException x) { //Old fashion: a constructor taking keys as arguments following the order of the keys
				Constructor<? extends T> constr = type.getConstructor(tkeyTypes);
				ret = constr.newInstance(keyValues);
			}
			return ret;
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
			PropertyManagement pm = PropertyManagement.getInstance();
			for (Field property : pm.getProperties(type)) {
				if (pm.isProperty(property) && ! this.isKey(property))
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
		
		if ((f.getModifiers()&Modifier.FINAL) != 0)
			throw new IllegalStateException("The key " + f + " should not be final.");
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
						throw new IllegalStateException("A key cannot be null as it is the case for key " + key + " of " + element);
//					if (key.getType().isArray() && Array.getLength(o) == 0)
//						throw new IllegalStateException("An array key cannot be empty as it is the case for key " + key + " of " + element);
					ret.append(ConversionTools.convertToString(o, key.getType()));
				}
				ret.append(KEY_END_SEPARATOR);
			}
			
			if (!element.getClass().equals(expected)) {
				ret.append(element.getClass().getName());
			}
			
			return ret.toString();
		} catch (RuntimeException x) {
			throw x;
		} catch (Exception x) {
			throw new RuntimeException(x);
		}
	}
	
	private volatile boolean PersistingElement.creatingIdentifier = false;

	public String PersistingElement.getIdentifier() {
		if (this.identifier == null && !this.creatingIdentifier) {
			this.creatingIdentifier = true;
			try {
				this.identifier = KeyManagement.getInstance().createIdentifier(this, this.getClass());
				if (this.identifier == null)
					throw new IllegalStateException("Element " + this + " has no identifier ; have all keys been set ?");
			} finally {
				this.creatingIdentifier = false;
			}
		}
		return this.identifier;
	}
	
	/**
	 * Checks whether this persisting element has a stable key.
	 * This means that each one of its keys is set to a valid and unchanged value.
	 */
	public void PersistingElement.checkIsValid() throws IllegalStateException {
		if (this.getIdentifier() == null)
			throw new IllegalStateException("Persisting element ot type " + this.getClass() + " is missing some of its key values.");
		String newKey = KeyManagement.getInstance().createIdentifier(this, getClass());
		if (!this.getIdentifier().equals(newKey)) {
			if (newKey == null)
				throw new IllegalArgumentException("One of the key value for " + this + " is no longer valid.");
			else
				throw new IllegalArgumentException("At leat a key for object " + this + " has changed (identifier would be now " + newKey + ")");
		}
	}

	public String PersistingElement.getFullIdentifier() {
		if (this.fullIdentifier == null) {
			this.fullIdentifier = KeyManagement.getInstance().createIdentifier(this, PersistingElement.class);
			if (this.fullIdentifier == null)
				throw new IllegalStateException("Element " + this + " has no identifier ; have all keys been set ?");
		}
		return this.fullIdentifier;
	}
}
