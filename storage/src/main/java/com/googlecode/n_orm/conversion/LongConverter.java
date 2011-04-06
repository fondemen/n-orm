package com.googlecode.n_orm.conversion;

class LongConverter extends NaturalConverter<Long> {
	public LongConverter() {
		super(Long.class, long.class, Long.SIZE/Byte.SIZE);
	}

	@Override
	public Long fromString(String rep, Class<?> expected) {
		return this.parseString(rep);
	}

	@Override
	public String toString(Long obj) {
		return this.unparseString(obj);
	}

	@Override
	public Long fromBytes(byte[] rep, Class<?> expected) {
		return this.parseBytes(rep);
	}

	@Override
	public byte[] toBytes(Long obj) {
		return this.unparseBytes(obj);
	}
}