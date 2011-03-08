package com.mt.storage.conversion;

class IntegerConverter extends NaturalConverter<Integer> {
	public IntegerConverter() {
		super(Integer.class, int.class, Integer.SIZE/Byte.SIZE);
	}

	@Override
	public Integer fromString(String rep, Class<?> expected) {
		return (int) this.parseString(rep);
	}

	@Override
	public String toString(Integer obj) {
		return this.unparseString(obj);
	}

	@Override
	public Integer fromBytes(byte[] rep, Class<?> expected) {
		return (int) this.parseBytes(rep);
	}

	@Override
	public byte[] toBytes(Integer obj) {
		return this.unparseBytes(obj);
	}
}