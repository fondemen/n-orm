package org.norm.conversion;

class ShortConverter extends NaturalConverter<Short> {
	public ShortConverter() {
		super(Short.class, short.class, Short.SIZE/Byte.SIZE);
	}

	@Override
	public Short fromString(String rep, Class<?> expected) {
		return (short) this.parseString(rep);
	}

	@Override
	public String toString(Short obj) {
		return this.unparseString(obj);
	}

	@Override
	public Short fromBytes(byte[] rep, Class<?> expected) {
		return (short) this.parseBytes(rep);
	}

	@Override
	public byte[] toBytes(Short obj) {
		return this.unparseBytes(obj);
	}
}