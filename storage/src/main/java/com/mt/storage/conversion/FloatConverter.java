package com.mt.storage.conversion;

class FloatConverter extends PrimitiveConverter<Float> {
	public static final int BYTE_SIZE = Float.SIZE/Byte.SIZE;
	public FloatConverter() {
		super(Float.class, float.class, BYTE_SIZE, BYTE_SIZE, 1, -1);
	}

	@Override
	public Float fromString(String rep, Class<?> expected) {
		throw new IllegalArgumentException("Cannot convert double to string.");
	}

	@Override
	public String toString(Float obj) {
		throw new IllegalArgumentException("Cannot convert double to string.");
	}

	@Override
	public Float fromBytes(byte[] rep, Class<?> expected) {
	    return Float.intBitsToFloat(ConversionTools.convert(Integer.class, rep).intValue());
	}

	@Override
	public byte[] toBytes(Float obj) {
	    return ConversionTools.convert(Float.floatToRawIntBits(obj));
	}

}