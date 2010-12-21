package com.mt.storage.conversion;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
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
				new DoubleConverter(), new KeyedElementConverter() };
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

				Set<Converter<?>> cachedConverters = knownConverters.get(clazz);
				if (cachedConverters == null) {
					cachedConverters = new HashSet<Converter<?>>();
					knownConverters.put(clazz, cachedConverters);
				}
				cachedConverters.add(conv);

				return ret;
			}
		throw new IllegalArgumentException(errorMessage);
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
				"Cannot create a bynary representation for " + o);
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
		StringTokenizer tkn = new StringTokenizer(id, separator);
		int i = 0;
		while (tkn.hasMoreTokens()) {
			int keys = KeyManagement.aspectOf().detectKeys(types[i]).size();
			String next = tkn.nextToken();
			while (keys > 1) {
				keys--;
				next += separator + tkn.nextToken();
			}
			ret[i] = convertFromString(types[i], next);
			i++;
		}
		return ret;
	}
}
