package com.googlecode.n_orm.conversion;

class EnumConverter extends SimpleConverter<Object> {
	public EnumConverter() {
		super(Object.class);
	}
	
	protected <U> U getEnumerated(String searched, Class<U> enumeration) {
		for (U enumerated : enumeration.getEnumConstants()) {
			if (enumerated.toString().equals(searched))
				return enumerated;
		}
		return null;
	}

	@Override
	public Object getDefaultValue() {
		return null;
	}

	@Override
	public Object fromString(String rep, Class<?> expected) {
		return getEnumerated(ConversionTools.stringConverter.fromString(rep, String.class), expected);
	}

	@Override
	public String toString(Object obj) {
		return ConversionTools.stringConverter.toString(obj.toString());
	}

	@Override
	public Object fromBytes(byte[] rep, Class<? extends Object> expected) {
		return getEnumerated(ConversionTools.stringConverter.fromBytes(rep, String.class), expected);
	}

	@Override
	public byte[] toBytes(Object obj) {
		return ConversionTools.stringConverter.toBytes(obj.toString());
	}

	@Override
	public boolean canConvert(Class<?> type) {
		return type.isEnum();
	}
}