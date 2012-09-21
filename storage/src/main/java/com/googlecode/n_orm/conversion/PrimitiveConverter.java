package com.googlecode.n_orm.conversion;

abstract class PrimitiveConverter<T> extends SimpleConverter<T> {
	private final Class<?> primitiveClazz;
	private final int minByteSize; 
	private final int maxByteSize; 
	private final int minStringSize; 
	private final int maxStringSize; 
	private final T primitiveDefaultValue;
	
	
	public PrimitiveConverter(Class<T> clazz, Class<?> primitiveClazz,
			int minByteSize, int maxByteSize, int minStringSize,
			int maxStringSize, T primitiveDefaultValue) {
		super(clazz);
		this.primitiveClazz = primitiveClazz;
		this.minByteSize = minByteSize;
		this.maxByteSize = maxByteSize;
		this.minStringSize = minStringSize;
		this.maxStringSize = maxStringSize;
		this.primitiveDefaultValue = primitiveDefaultValue;
	}

	public Class<?> getPrimitiveClazz() {
		return this.primitiveClazz;
	}

	public int getMinByteSize() {
		return minByteSize;
	}

	public int getMaxByteSize() {
		return maxByteSize;
	}

	public int getMinStringSize() {
		return minStringSize;
	}

	public int getMaxStringSize() {
		return maxStringSize;
	}

	@Override
	public T getDefaultValue(Class<?> type) {
		if (this.getClazz().isAssignableFrom(type))
			return null; //It's the non-primitive type (e.g. Integer and not int)
		return primitiveDefaultValue;
	}

	@Override
	protected boolean canConvert(byte[] rep) {
		return super.canConvert(rep) && this.getMinByteSize() <= rep.length && rep.length <= this.getMaxByteSize();
	}

	@Override
	protected boolean canConvert(String rep) {
		return super.canConvert(rep) && this.getMinStringSize() <= rep.length() && rep.length() <= this.getMaxStringSize();
	}

	@Override
	public boolean canConvert(Class<?> type) {
		return super.canConvert(type) || this.getPrimitiveClazz().equals(type);
	}

	@Override
	public void checkInstance(T obj, Class<? extends T> expected) {
		if (this.getPrimitiveClazz().equals(expected))
			expected = this.getClazz();
		super.checkInstance(obj, expected);
	}
	
	public T revert(T obj) {
		throw new UnreversibleTypeException(obj.getClass());
	}

	@Override
	public Object fromStringReverted(String rep, Class<?> type) {
		return this.revert(this.fromString(rep, type));
	}

	@Override
	public String toStringReverted(T obj, Class<? extends T> expected) {
		return this.toString(this.revert(obj), expected);
	}
	
}