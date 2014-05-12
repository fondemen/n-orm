package com.googlecode.n_orm;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.cache.perthread.Cache;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.conversion.UnreversibleTypeException;


/**
 * To be identifiable, a persisting object must define fields to setup the key by annotating them with {@link Key}.
 * Keys must declare an order from 1 to N, must not overlap, and must be all there, e.g. if N=3, the class must declare keys for orders 1, 2 and 3.
 * Keys are inherited.
 * For non-persisting elements (i.e. elements that are not annotated with {@link Persisting}, all fields must be keys.
 * As such, an element may be identified by a string, whose format is the following:
 * <code>
 * id ::=   sid //for a simple element such as a character string, an integer, an enum...<br>
 *        | id ( {@link KeyManagement#KEY_SEPARATOR} id)* ({@link KeyManagement#KEY_END_SEPARATOR} sid)? '}' //for an element as decribed above ; each included id representing its key values, and the last optional sid representing its class<br>
 *        | id ( {@link KeyManagement#ARRAY_SEPARATOR}  id)* //for an array<br>
 * sid ::= (~({@link KeyManagement#KEY_SEPARATOR}|{@link KeyManagement#KEY_END_SEPARATOR}|{@link KeyManagement#ARRAY_SEPARATOR}))*
 * </code>
 * @author fondemen
 *
 */
public aspect KeyManagement {
	public static final String KEY_SEPARATOR = "\u0017";  //Shouldn't be a printable char
	public static final String KEY_END_SEPARATOR = "\u0001"; //As small as possible so that {v="AA"}.identifier (= "AA"+KEY_END_SEPARATOR) < {v="AAA"}.identifier (= "AAA"+KEY_END_SEPARATOR)
	public static final String ARRAY_SEPARATOR = "\uFFFF"; //As large as possible so that [0, 1] (identified by 0 + StringSeparator + 1) < [0, 1] (identified by 0 + StringSeparator + 1 + StringSeparator + 2)
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
	
	declare error: set(@Key(reverted=true) (!java.util.Date && !boolean && !Boolean && !byte && !Byte && !short && !Short &&!int && !Integer && !long && !Long) *.*) : "Can only revert boolean, natural or java.util.Date keys";
	
	//declare @field: * *.* : @java.lang.SuppressWarnings(value={"unused"}) ;
	
	private static class DecomposableString {
		private static final String keySeparator;
		private static final String arraySeparator;
		private static final String keyEndSeparator;
		private static final String[] specialChars;
		
		static {
			keySeparator = KEY_SEPARATOR;
			arraySeparator = ARRAY_SEPARATOR;
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
			U ret = this.detectLast(type, false);
			if (! this.isEmpty()) {
				throw new IllegalArgumentException("Could not analyze the complete string: " + this.rest + " left over while analyzing " + this.ident + " as a " + type + " instance.");
			}
			return ret;
		}
		
		protected <U> U detectLast(Class<U> expected, boolean revert) {
			if (expected.isArray()) {
				if  (revert) throw new IllegalArgumentException("Cannot revert an array such as " + expected.getName());
				return detectLastArray(expected);
			} else {
				if (km.detectKeys(expected).size() > 0) {
					if (revert) throw new IllegalArgumentException("Cannot revert a keyed element such as " + expected.getName());
					return detectLastKeyedElement(expected);
				} else {
					return detectLastSimpleElement(expected, revert);
				}
			}
		}
		
		private <U> U detectLastArray(Class<U> expected) {
			Class<?> componentType = expected.getComponentType();
			LinkedList<Object> elements = new LinkedList<Object>();
			String restSav = this.rest;
			do {
				try {
					elements.addFirst(this.detectLast(componentType, false));
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
				try {
					vals[i] = this.detectLast(keys.get(i).getType(), keys.get(i).getAnnotation(Key.class).reverted());
				} catch (UnreversibleTypeException x) {
					throw new UnreversibleTypeException("Key " + keys.get(i) + " cannot be reverted", x.getType(), x);
				}
				if (i > 0)
					this.checkLast(keySeparator);
			}
			ident = ident.substring(this.rest.length());
			U ret = null;
			try {
				ret = (U) km.getKnownPersistingElement(ident, (Class<? extends PersistingElement>) actualType);
			} catch (Exception x) {}
			if (ret == null) {
				ret = km.createElement(actualType, vals);
				if (ret instanceof PersistingElement) {
					((PersistingElement)ret).identifier = ident;
					((PersistingElement)ret).getFullIdentifier();
					km.register((PersistingElement) ret);
				}
			}
			return ret;
		}
		
		private <U> U detectLastSimpleElement(Class<U> expected, boolean revert) {
			if (rest.endsWith(keyEndSeparator))
				throw new IllegalArgumentException("Detecting complex type at the end of " + this.rest + " while expecting element of simple type " + expected);
			return revert ?
					ConversionTools.convertFromStringReverted(expected, this.detectLastSimpleId())
					: ConversionTools.convertFromString(expected, this.detectLastSimpleId());
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
	
	public void register(PersistingElement element) {
		Cache.getCache().register(element);
	}
	
	public void unregister(PersistingElement element) {
		Cache.getCache().unregister(element);
	}
	
	public void unregister(Class<? extends PersistingElement> clazz, String id) {
		if (! id.endsWith(clazz.getName())) {
			id = id + clazz.getName();
		}
		if (! id.endsWith("" + KEY_END_SEPARATOR + clazz.getName())) {
			throw new IllegalArgumentException(id + " is not a valid identifier");
		}
		Cache.getCache().unregister(id);
	}
	
	public PersistingElement getKnownPersistingElement(String fullIdentifier) {
		return Cache.getCache().getKnownPersistingElement(fullIdentifier);
	}
	
	public PersistingElement getKnownPersistingElement(String identifier, Class<? extends PersistingElement> clazz) {
		return this.getKnownPersistingElement(identifier + clazz.getName());
	}
	
	//For test purpose
	public void cleanupKnownPersistingElements() {
		Cache.getCache().reset();
	}
	
	after(PersistingElement self) returning: execution(void PersistingElement+.delete()) && this(self) {
		this.unregister(self);
	}
	
	/**
	 * Creates an element of the expected type with the given id.
	 * Id may be a simple id or a full id (including reference to the actual type).
	 * Elements are cached using a per-thread cache (see {@link Cache}).
	 */
	public <T> T createElement(Class<T> expectedType, String id) {
		try {
			return new DecomposableString(id).detect(expectedType);
		} catch (Exception x) {
			throw new IllegalArgumentException("Cannot create instance of " + expectedType + " with id " + id + ": " + x.getMessage(), x);
		}
	}
	
	<T> T createElement(Class<T> type, Object [] keyValues) {
		
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
			throw new RuntimeException("Cannot build new instance of " + type + " with key values " + Arrays.toString(strVals), x);
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
		return this.createIdentifier(element, expected, true);
	}
	
	public String createIdentifier(Object element, Class<?> expected, boolean canCheckCache) {
		if (expected != null && ! expected.isInstance(element))
			throw new ClassCastException("Element " + element + " of class " + element.getClass() + " is not compatible with " + expected);
		try {
			StringBuffer ret = new StringBuffer();
			
			if (canCheckCache && (element instanceof PersistingElement) && ((PersistingElement)element).identifier != null) {
				ret.append(((PersistingElement)element).identifier);
			} else {
				boolean fst = true;
				PropertyManagement pm = PropertyManagement.getInstance();
				for (Field key : this.detectKeys(element.getClass())) {
					if (fst) fst = false; else ret.append(KEY_SEPARATOR);
					Object o = pm.readValue(element, key);
					if (o == null)
						throw new IllegalStateException("A key cannot be null as it is the case for key " + key + " of " + element);
//					if (key.getType().isArray() && Array.getLength(o) == 0)
//						throw new IllegalStateException("An array key cannot be empty as it is the case for key " + key + " of " + element);
					if (key.getAnnotation(Key.class).reverted())
						ret.append(ConversionTools.convertToStringReverted(o, key.getType()));
					else
						ret.append(ConversionTools.convertToString(o, key.getType()));
				}
				ret.append(KEY_END_SEPARATOR);
			}
			
			if (expected != null && !element.getClass().equals(expected)) {
				ret.append(element.getClass().getName());
			}
			
			return ret.toString();
		} catch (RuntimeException x) {
			throw x;
		} catch (Exception x) {
			throw new RuntimeException(x);
		}
	}
	
	private volatile transient boolean PersistingElement.creatingIdentifier = false;
	
	/**
	 * Checks whether this persisting element has a stable key.
	 * This means that each one of its keys is set to a valid and unchanged value.
	 */
	public void PersistingElement.checkIsValid() throws IllegalStateException {
		if (this.getIdentifier() == null)
			throw new IllegalStateException("Persisting element ot type " + this.getClass() + " is missing some of its key values.");
		KeyManagement km = KeyManagement.getInstance();
		String newKey = km.createIdentifier(this, getClass(), false);
		if (!this.getIdentifier().equals(newKey)) {
			km.unregister(this);
			if (newKey == null)
				throw new IllegalStateException("One of the key value for " + this + " is no longer valid.");
			else
				throw new IllegalStateException("At least a key for object " + this + " has changed (identifier would be now " + newKey + ")");
		}
		km.register(this);
	}

	public String PersistingElement.getIdentifier() {
		if (this.identifier == null && !this.creatingIdentifier) {
			this.creatingIdentifier = true;
			try {
				this.identifier = KeyManagement.getInstance().createIdentifier(this, this.getClass());
				if (this.identifier == null)
					throw new IllegalStateException("Element " + this + " has no identifier ; have all keys been set ?");
				this.getFullIdentifier(); //Must be created as soon as this function is valid
			} finally {
				this.creatingIdentifier = false;
			}
		}
		return this.identifier;
	}

	public String PersistingElement.getFullIdentifier() {
		if (this.fullIdentifier == null) {
			this.fullIdentifier = KeyManagement.getInstance().createIdentifier(this, PersistingElement.class);
			if (this.fullIdentifier == null)
				throw new IllegalStateException("Element " + this + " has no identifier ; have all keys been set ?");
			this.getIdentifier(); //Must be created as soon as this function is valid
		}
		return this.fullIdentifier;
	}
}
