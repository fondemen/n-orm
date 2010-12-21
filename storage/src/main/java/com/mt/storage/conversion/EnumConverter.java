package com.mt.storage.conversion;

class EnumConverter extends Converter<Object> {
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
	public Object fromString(String rep, Class<?> expected) {
		return getEnumerated(ConversionTools.convertFromString(String.class, rep), expected);
	}

	@Override
	public String toString(Object obj) {
		return ConversionTools.convertToString(obj.toString());
	}

	@Override
	public Object fromBytes(byte[] rep, Class<? extends Object> expected) {
		return getEnumerated(ConversionTools.convert(String.class, rep), expected);
	}

	@Override
	public byte[] toBytes(Object obj) {
		return ConversionTools.convert(obj.toString());
	}

	@Override
	public boolean canConvert(Class<?> type) {
		return type.isEnum();
	}
}