package com.mt.storage.conversion;

import java.util.Date;

class DateConverter extends Converter<Date> {
	public DateConverter() {
		super(Date.class);
	}

	@Override
	public Date fromString(String rep, Class<?> expected) {
		return new Date(ConversionTools.convertFromString(Long.class, rep));
	}

	@Override
	public String toString(Date obj) {
		return ConversionTools.convertToString(obj.getTime());
	}

	@Override
	public Date fromBytes(byte[] rep, Class<? extends Object> expected) {
		return new Date(ConversionTools.convert(Long.class, rep));
	}

	@Override
	public byte[] toBytes(Date obj) {
		return ConversionTools.convert(obj.getTime());
	}
}