package com.googlecode.n_orm.conversion;

class ByteConverter extends NaturalConverter<Byte> {
	public ByteConverter() {
		super(Byte.class, byte.class, Byte.SIZE/Byte.SIZE, (byte)0);
	}

	@Override
	public Byte fromString(String rep, Class<?> expected) {
		return (byte) this.parseString(rep);
	}

	@Override
	public String toString(Byte obj) {
		return this.unparseString(obj);
	}

	@Override
	public Byte fromBytes(byte[] rep, Class<?> expected) {
		return (byte) this.parseBytes(rep);
	}

	@Override
	public byte[] toBytes(Byte obj) {
		return this.unparseBytes(obj);
	}

	@Override
	public Byte revert(Byte obj) {
		return (byte) -obj;
	}
}