package com.mt.storage.conversion;

import java.util.Date;

import com.mt.storage.KeyManagement;

class KeyedElementConverter extends Converter<Object> {
	public KeyedElementConverter() {
		super(Object.class);
	}

	@Override
	public Object fromString(String rep, Class<?> expected) {
		return KeyManagement.aspectOf().createElement(expected, rep);
	}

	@Override
	public String toString(Object obj) {
		return KeyManagement.aspectOf().createIdentifier(obj);
	}

	@Override
	public Object fromBytes(byte[] rep, Class<? extends Object> expected) {
		return KeyManagement.aspectOf().createElement(expected, ConversionTools.stringConverter.fromBytes(rep, String.class));
	}

	@Override
	public byte[] toBytes(Object obj) {
		return ConversionTools.convert(KeyManagement.aspectOf().createIdentifier(obj));
	}

	@Override
	public boolean canConvert(Class<?> type) {
		return KeyManagement.aspectOf().canCreateFromKeys(type);
	}
}