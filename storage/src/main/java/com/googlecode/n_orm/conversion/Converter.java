package com.googlecode.n_orm.conversion;

abstract class Converter<T> implements
		Comparable<Converter<?>> {
	private final Class<T> clazz;

	public Converter(Class<T> clazz) {
		this.clazz = clazz;
	}

	public Class<T> getClazz() {
		return this.clazz;
	}

	public abstract T fromString(String rep, Class<?> expected);
	
	public abstract String toString(T obj, Class<? extends T> expected);

	public abstract T fromBytes(byte[] rep, Class<?> type);

	public abstract byte[] toBytes(T obj, Class<? extends T> expected);

	public boolean canConvertToBytes(Object obj) {
		return this.canConvert(obj.getClass());
	}

	public boolean canConvertToString(Object obj) {
		return this.canConvert(obj.getClass());
	}
	
	public boolean canConvert(Class<?> type) {
		return this.getClazz().equals(type);
	}
	
	protected boolean canConvert(byte [] obj) {
		return true;
	}
	
	protected boolean canConvert(String obj) {
		return true;
	}

	public boolean canConvertFromBytes(byte [] obj, Class<?> expected) {
		return this.canConvert(obj) && this.canConvert(expected);
	}

	public boolean canConvertFromString(String obj, Class<?> expected) {
		return this.canConvert(obj) && this.canConvert(expected);
	}

	@Override
	public int compareTo(Converter<?> rhs) {
		return this.getClazz().getName()
				.compareTo(rhs.getClazz().getName());
	}

	public abstract T getDefaultValue(Class<?> type);

	public Object fromStringReverted(String rep, Class<?> type) {
		throw new UnreversibleTypeException(type);
	}

	public String toStringReverted(T obj, Class<? extends T> expected) {
		throw new UnreversibleTypeException(expected);
	}
}