package com.googlecode.n_orm.conversion;

import com.googlecode.n_orm.KeyManagement;

class KeyedElementConverter extends Converter<Object> {
	private final KeyManagement keyManager = KeyManagement.getInstance();
	
	public KeyedElementConverter() {
		super(Object.class);
	}

	@Override
	public Object getDefaultValue() {
		return null;
	}

	@Override
	public Object fromString(String rep, Class<?> expected) {
		return keyManager.createElement(expected, rep);
	}

	@Override
	public String toString(Object obj, Class<?> expected) {
		return keyManager.createIdentifier(obj, expected);
	}

	@Override
	public Object fromBytes(byte[] rep, Class<? extends Object> expected) {
		return keyManager.createElement(expected, ConversionTools.stringConverter.fromBytes(rep, String.class));
	}

	@Override
	public byte[] toBytes(Object obj, Class<?> expected) {
		return ConversionTools.stringConverter.toBytes(this.toString(obj, expected));
	}

	@Override
	public boolean canConvert(Class<?> type) {
		return KeyManagement.getInstance().canCreateFromKeys(type);
	}
}