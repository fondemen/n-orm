package com.googlecode.n_orm.conversion;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.PropertyManagement;


public class ConversionTools {

	private static enum ConversionKind {
		FromBytes, ToBytes, FromString, ToString, FromStringReverted, ToStringReverted, Default
	};

	private static final Converter<?>[] converters;
	static final StringConverter stringConverter;
	static final LongConverter longConverter;
	static final IntegerConverter intConverter;
	static final ByteConverter byteConverter;

	private static final Map<Class<?>, Set<Converter<?>>> knownConverters = new HashMap<Class<?>, Set<Converter<?>>>();

	static {
		stringConverter = new StringConverter();
		longConverter = new LongConverter();
		intConverter = new IntegerConverter();
		byteConverter = new ByteConverter();
		converters = new Converter<?>[] { stringConverter,
				new PersistingConverter(), new EnumConverter(),
				new DateConverter(), new BooleanConverter(),
				new CharacterConverter(), byteConverter,
				new ShortConverter(), intConverter,
				longConverter, new FloatConverter(),
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
		case FromStringReverted:
			return conv.canConvertFromString((String) o, type);
		case ToString:
		case ToStringReverted:
			return conv.canConvertToString(o);
		case Default:
			return conv.canConvert(type);

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
			return conv.toBytes((T) o, (Class<? extends T>) type);
		case FromString:
			return conv.fromString((String) o, type);
		case ToString:
			return conv.toString((T) o, (Class<? extends T>) type);
		case FromStringReverted:
			return conv.fromStringReverted((String) o, type);
		case ToStringReverted:
			return conv.toStringReverted((T) o, (Class<? extends T>) type);
		case Default:
			return conv.getDefaultValue((Class<? extends T>) type);
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

	@SuppressWarnings("unchecked")
	public static <U> U convertFromStringReverted(Class<U> type, String representation) {
		if (representation == null)
			return null;

		return (U) convertInternal(representation, type,
				ConversionKind.FromStringReverted, "Cannot create a " + type
						+ " from reverted string " + representation);
	}
	
	public static  byte[] convert(Object o) {
		return convert(o, o == null ? Object.class : o.getClass());
	}

	public static  byte[] convert(Object o, Class<?> expected) {
		if (o instanceof PropertyManagement.Property)
			o = ((PropertyManagement.Property) o).getValue();

		if (o == null)
			return null;
		
		return (byte[]) convertInternal(o, expected, ConversionKind.ToBytes,
				"Cannot create a binary representation for " + o + " of class " + o.getClass() + " while expecting " + expected.getName());
	}
	
	public static String convertToString(Object o) {
		return convertToString(o, o == null ? Object.class : o.getClass());
	}

	public static String convertToString(Object o, Class<?> expected) {

		if (o == null)
			return null;

		return (String) convertInternal(o, expected, ConversionKind.ToString,
				"Cannot create a string representation for " + o);
	}

	public static String convertToStringReverted(Object o, Class<?> expected) {

		if (o == null)
			return null;

		return (String) convertInternal(o, expected, ConversionKind.ToStringReverted,
				"Cannot create a reverted string representation for " + o);
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T getDefaultValue(Class<T> expected) {
		return (T) convertInternal(null, expected, ConversionKind.Default, null);
	}
}
