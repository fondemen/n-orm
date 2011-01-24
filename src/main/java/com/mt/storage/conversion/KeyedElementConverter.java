package com.mt.storage.conversion;

import com.mt.storage.KeyManagement;

class KeyedElementConverter extends Converter<Object> {
	public KeyedElementConverter() {
		super(Object.class);
	}

	@Override
	public Object fromString(String rep, Class<?> expected) {
		return KeyManagement.getInstance().createElement(expected, rep);
	}

	@Override
	public String toString(Object obj) {
		return KeyManagement.getInstance().createIdentifier(obj);
	}

	@Override
	public Object fromBytes(byte[] rep, Class<? extends Object> expected) {
		return KeyManagement.getInstance().createElement(expected, ConversionTools.stringConverter.fromBytes(rep, String.class));
	}

	@Override
	public byte[] toBytes(Object obj) {
		return ConversionTools.convert(KeyManagement.getInstance().createIdentifier(obj));
	}

	@Override
	public boolean canConvert(Class<?> type) {
		return KeyManagement.getInstance().canCreateFromKeys(type);
	}
}