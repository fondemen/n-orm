package com.mt.storage.conversion;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import com.mt.storage.KeyManagement;
import com.mt.storage.PropertyManagement;

public class ConversionTools {

	private static enum ConversionKind {
		FromBytes, ToBytes, FromString, ToString
	};

	private static final Converter<?>[] converters;
	static final StringConverter stringConverter;

	private static final Map<Class<?>, Set<Converter<?>>> knownConverters = new HashMap<Class<?>, Set<Converter<?>>>();

	static {
		stringConverter = new StringConverter();
		converters = new Converter<?>[] { stringConverter,
				new PersistingConverter(), new EnumConverter(),
				new DateConverter(), new BooleanConverter(),
				new CharacterConverter(), new ByteConverter(),
				new ShortConverter(), new IntegerConverter(),
				new LongConverter(), new FloatConverter(),
				new DoubleConverter(), new ArrayConverter(), new KeyedElementConverter() };
	}

	private static boolean test(Converter<?> conv, Object o, Class<?> type,
			ConversionKind kind) {
		switch (kind) {
		case FromBytes:
			return conv.canConvertFromBytes((byte[]) o, type);
		case ToBytes:
			return conv.canConvertToBytes(o);
		case FromString:
			return conv.canConvertFromString((String) o, type);
		case ToString:
			return conv.canConvertToString(o);

		default:
			throw new IllegalArgumentException("Unknown conversion kind: "
					+ kind.name());
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> Object convert(Converter<T> conv, Object o,
			Class<?> type, ConversionKind kind) {
		switch (kind) {
		case FromBytes:
			return conv.fromBytes((byte[]) o, type);
		case ToBytes:
			return conv.toBytes((T) o);
		case FromString:
			return conv.fromString((String) o, type);
		case ToString:
			return conv.toString((T) o);

		default:
			throw new IllegalArgumentException("Unknown conversion kind: "
					+ kind.name());
		}
	}
	
	public static boolean canConvert(Class<?> clazz) {
		if (clazz == null)
			return false;
		
		if (knownConverters.containsKey(clazz))
			return true;
		
		for (Converter<?> conv : converters) {
			if (conv.canConvert(clazz)) {
				registerConverter(clazz, conv);
				return true;
			}
		}
		
		return false;
	}

	private static Object convertInternal(Object o, Class<?> type,
			ConversionKind kind, String errorMessage) {
		Class<?> clazz = type == null ? o.getClass() : type;
		if (knownConverters.containsKey(clazz)) {
			for (Converter<?> conv : knownConverters.get(clazz)) {
				if (test(conv, o, type, kind))
					return convert(conv, o, type, kind);
			}
		}

		for (Converter<?> conv : converters)
			if (test(conv, o, type, kind)) {
				Object ret = convert(conv, o, type, kind);
				registerConverter(clazz, conv);
				return ret;
			}
		throw new IllegalArgumentException(errorMessage);
	}

	private static void registerConverter(Class<?> clazz, Converter<?> conv) {
		Set<Converter<?>> cachedConverters = knownConverters.get(clazz);
		if (cachedConverters == null) {
			cachedConverters = new HashSet<Converter<?>>();
			knownConverters.put(clazz, cachedConverters);
		}
		cachedConverters.add(conv);
	}

	@SuppressWarnings("unchecked")
	public static <U> U convert(Class<U> type, byte[] representation) {

		if (representation == null)
			return null;

		return (U) convertInternal(
				representation,
				type,
				ConversionKind.FromBytes,
				"Cannot create a "
						+ type
						+ " from byte array "
						+ stringConverter.fromBytes(representation,
								String.class));

	}

	@SuppressWarnings("unchecked")
	public static <U> U convertFromString(Class<U> type, String representation) {
		if (representation == null)
			return null;

		return (U) convertInternal(representation, type,
				ConversionKind.FromString, "Cannot create a " + type
						+ " from string " + representation);
	}

	public static byte[] convert(Object o) {
		if (o instanceof PropertyManagement.Property)
			o = ((PropertyManagement.Property) o).getValue();

		if (o == null)
			return null;

		return (byte[]) convertInternal(o, null, ConversionKind.ToBytes,
				"Cannot create a binary representation for " + o);
	}

	public static String convertToString(Object o) {

		if (o == null)
			return null;

		return (String) convertInternal(o, null, ConversionKind.ToString,
				"Cannot create a string representation for " + o);
	}

	public static Object[] decompose(String id, String separator,
			Class<?>[] types) {
		Object[] ret = new Object[types.length];
		DecomposableString tkn = new DecomposableString(id);
		int i = 0;
		KeyManagement km = KeyManagement.getInstance();
		while (!tkn.isEmpty()) {
			if (i > 0) {
				tkn.detect(separator);
			}
			Class<?> type = types[i];
			if (type.isArray()) {
				type = type.getComponentType();
				int keys = km.detectKeys(type).size();
				String innerSep = km.getSeparator(type);
				String [] reps = tkn.detectArray(keys, separator, innerSep);
				Object[] objs = (Object[]) Array.newInstance(type, reps.length);
				for(int ri = 0; ri < reps.length; ++ri) {
					objs[ri] = convertFromString(type, reps[ri]);
				}
				ret[i] = objs;
			} else {
				int keys = km.detectKeys(type).size();
				String rep = tkn.detectKey(keys, separator);
				ret[i] = convertFromString(type, rep);
			}
			i++;
		}
		return ret;
	}
	
	private static class DecomposableString {
		private static final KeyManagement km = KeyManagement.getInstance();
		private static final String arraySeparator = ArrayConverter.StringSeparator;
		private final String ident;
		private String rest;
		
		public DecomposableString(String ident) {
			this.ident = ident;
			this.rest = ident;
		}
		
		public boolean isEmpty() {
			return this.rest.isEmpty();
		}
		
		public int getKeySize(Class<?> clazz) {
			return km.detectKeys(clazz).size();
		}
		
		public String getSeparator(Class<?> clazz) {
			return km.getSeparator(clazz);
		}
		
		public void detect(String start) {
			if (this.rest.startsWith(start)) {
				this.rest = this.rest.substring(start.length());
			} else
				throw new IllegalArgumentException("Expecting " + start + " before " + this.rest + " on identifier " + this.ident);
		}
		
		public String detectKey(int keySize, String keySeparator) {
			if (keySize == 0) keySize = 1;
			StringTokenizer st = new StringTokenizer(rest, keySeparator);
			StringBuffer ret = new StringBuffer();
			for(;keySize > 0; keySize--) {
				String nextToken = st.nextToken();
				if (keySize > 1) {
					ret.append(nextToken);
					ret.append(keySeparator);
				} else { //last part of the key ; in case we are in an array, the key should be shortened up to the array separator
					int arraySep = nextToken.indexOf(arraySeparator);
					if (arraySep > 0) {
						nextToken = nextToken.substring(0, arraySep);
					}
					ret.append(nextToken);
				}
			}
			String res = ret.toString();
			this.rest = this.rest.substring(res.length());
			return res;
		}
		
		public String[] detectArray(int keySize, String outerKeySeparator, String innerKeySeparator) {
			List<String> ret = new LinkedList<String>();
			boolean first = true;
			while (! this.rest.isEmpty() && ! this.rest.startsWith(outerKeySeparator)) {
				if (first) {
					assert !this.rest.startsWith(arraySeparator);
					first = false;
				} else {
					this.detect(arraySeparator);
				}
				String elementString = this.detectKey(keySize, innerKeySeparator);
				ret.add(elementString);
			}
			return ret.toArray(new String[ret.size()]);
		}
	}
}
