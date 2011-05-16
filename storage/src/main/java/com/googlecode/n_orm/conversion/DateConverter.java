package com.googlecode.n_orm.conversion;

import java.util.Date;

class DateConverter extends SimpleConverter<Date> {
	public DateConverter() {
		super(Date.class);
	}

	@Override
	public Date getDefaultValue() {
		return null;
	}

	@Override
	public Date fromString(String rep, Class<?> expected) {
		return new Date(ConversionTools.longConverter.parseString(rep));
	}

	@Override
	public String toString(Date obj) {
		return ConversionTools.longConverter.unparseString(obj.getTime());
	}

	@Override
	public Date fromBytes(byte[] rep, Class<? extends Object> expected) {
		return new Date(ConversionTools.longConverter.parseBytes(rep));
	}

	@Override
	public byte[] toBytes(Date obj) {
		return ConversionTools.longConverter.unparseBytes(obj.getTime());
	}
}