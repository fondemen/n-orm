package com.mt.storage.conversion;

class DoubleConverter extends PrimitiveConverter<Double> {
	public static final int BYTE_SIZE = Double.SIZE/Byte.SIZE;

	public DoubleConverter() {
		super(Double.class, double.class, BYTE_SIZE, BYTE_SIZE, 1, -1);
	}

	@Override
	public Double fromString(String rep, Class<?> expected) {
		throw new IllegalArgumentException("Cannot convert double to string.");
	}

	@Override
	public String toString(Double obj) {
		throw new IllegalArgumentException("Cannot convert double to string.");
	}

	@Override
	public Double fromBytes(byte[] rep, Class<?> expected) {
	    return Double.longBitsToDouble(ConversionTools.convert(Long.class, rep).longValue());
	}

	@Override
	public byte[] toBytes(Double obj) {
	    return ConversionTools.convert(Double.doubleToRawLongBits(obj));
	}

}