package com.mt.storage.conversion;

public abstract class SimpleConverter<T> extends Converter<T> {

	public SimpleConverter(Class<T> clazz) {
		super(clazz);
	}

	protected abstract String toString(T obj);
	
	@Override
	public String toString(T obj, Class<? extends T> expected) {
		checkInstance(obj, expected);
		return this.toString(obj);
	}

	public void checkInstance(T obj, Class<? extends T> expected) {
		if (! expected.isInstance(obj))
			throw new IllegalArgumentException("Incompatible type: expecting " + expected + " while got object " + obj + " of class " + obj.getClass());
	}

	protected abstract byte[] toBytes(T obj);

	@Override
	public byte[] toBytes(T obj, Class<? extends T> expected) {
		checkInstance(obj, expected);
		return this.toBytes(obj);
	}
}
