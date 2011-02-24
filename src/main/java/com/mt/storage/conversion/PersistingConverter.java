package com.mt.storage.conversion;

import com.mt.storage.KeyManagement;
import com.mt.storage.PersistingElement;

class PersistingConverter extends Converter<PersistingElement> {
	public PersistingConverter() {
		super(PersistingElement.class);
	}

	@Override
	public PersistingElement fromString(String rep, Class<?> expected) {
		return (PersistingElement) KeyManagement.getInstance().createElement(expected, rep);
	}

	@Override
	public String toString(PersistingElement obj) {
		return obj.getIdentifier();
	}

	@Override
	public PersistingElement fromBytes(byte[] rep, Class<?> expected) {
		return this.fromString(ConversionTools.stringConverter.fromBytes(rep, String.class), expected);
	}

	@Override
	public byte[] toBytes(PersistingElement obj) {
		return ConversionTools.stringConverter.toBytes(this.toString(obj));
	}

	@Override
	public boolean canConvert(Class<?> type) {
		return this.getClazz().isAssignableFrom(type);
	}

}