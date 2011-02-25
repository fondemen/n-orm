package com.mt.storage.conversion;

import java.lang.reflect.Array;
import java.util.StringTokenizer;

public class ArrayConverter extends Converter<Object> {
	public static final String StringSeparator = "\uFFFF"; //As large as possible so that [0, 1] (identified by 0 + StringSeparator + 1) < [0, 1] (identified by 0 + StringSeparator + 1 + StringSeparator + 2)

	//public static final String StringEndSeparator = "]";
	private static int IntBytesLength = -1;
	
	public static int getIntBytesLength() {
		if (IntBytesLength == -1)
			IntBytesLength = ConversionTools.intConverter.toBytes(Integer.MAX_VALUE).length;
		return IntBytesLength;
	}

	public ArrayConverter() {
		super(Object.class);
	}

	@Override
	public Object fromString(String rep, Class<?> expected) {
		Class<?> clazz = expected.getComponentType();
		if (clazz.isArray())
			throw new IllegalArgumentException("Cannot convert to string a multidimensional array as " + expected);
		StringTokenizer t = new StringTokenizer(rep, StringSeparator);
		int length = t.countTokens();
		Object ret = Array.newInstance(clazz, length);
		for (int i = 0; i < length; i++) {
			Array.set(ret, i, ConversionTools.convertFromString(clazz, t.nextToken()));
		}
		return ret;
	}

	@Override
	public String toString(Object obj, Class<?> expected) {
		StringBuffer sb = new StringBuffer();
		boolean first = true;
		Class<?> expectedComponent = expected.getComponentType();
		if (expectedComponent.isArray())
			throw new IllegalArgumentException("Cannot convert to string a multidimensional array as " + expected);
		for (int i = 0; i < Array.getLength(obj); i++) {
			if (first)
				first = false;
			else
				sb.append(StringSeparator);
			sb.append(ConversionTools.convertToString(Array.get(obj, i), expectedComponent));
		}
		return sb.toString();
	}

	@Override
	public Object fromBytes(byte[] rep, Class<?> type) {
		if (rep.length == 0)
			return null;
		
		if (type.equals(byte[].class))
			return rep;
		
		Class<?> clazz = type.getComponentType();
		byte [] tmpBytes = new byte [getIntBytesLength()];
		System.arraycopy(rep, 0, tmpBytes, 0, getIntBytesLength());
		int pos = getIntBytesLength();
		int length = ConversionTools.intConverter.fromBytes(tmpBytes, int.class), objLength;
		Object ret = Array.newInstance(type.getComponentType(), length);
		for (int i = 0; i < length; ++i) {
			tmpBytes = new byte [getIntBytesLength()];
			System.arraycopy(rep, pos, tmpBytes, 0, getIntBytesLength());
			pos += getIntBytesLength();
			objLength = ConversionTools.intConverter.fromBytes(tmpBytes, int.class);
			tmpBytes = new byte[objLength];
			System.arraycopy(rep, pos, tmpBytes, 0, objLength);
			pos+= objLength;
			Array.set(ret, i, ConversionTools.convert(clazz, tmpBytes));
		}
		
		return ret;
	}

	@Override
	public byte[] toBytes(Object object, Class<?> expected) {
		if (object == null)
			return new byte [0];
		
		if(object instanceof byte[])
			return (byte[]) object;
		
		int objLength = Array.getLength(object);
		if (objLength == 0)
			return ConversionTools.intConverter.toBytes(0);
		
		byte [] [] elements = new byte [1+(2*objLength)] [];
		int bytes = 0;
		elements[0] = ConversionTools.intConverter.toBytes(objLength);
		bytes += elements[0].length;
		Class<?> expectedComponent = expected.getComponentType();
		for (int i = 0, eltI = 1; i < objLength; i++, eltI+=2) {
			elements[eltI+1] = ConversionTools.convert(Array.get(object, i), expectedComponent);
			elements[eltI] = ConversionTools.intConverter.toBytes(elements[eltI+1].length);
			assert elements[eltI].length == getIntBytesLength();
			bytes += getIntBytesLength() + elements[eltI+1].length;
		}
		
		byte[] ret = new byte[bytes];
		int destPos = 0;
		for (int i = 0; i < elements.length; i++) {
			System.arraycopy(elements[i], 0, ret, destPos, elements[i].length);
			destPos += elements[i].length;
		}
		
		return ret;
	}

	@Override
	public boolean canConvert(Class<?> type) {
		return type.isArray() && ConversionTools.canConvert(type.getComponentType());
	}

	@Override
	public boolean canConvertToString(Object obj) {
		return super.canConvertToString(obj) && !obj.getClass().getComponentType().isArray();
	}

}
