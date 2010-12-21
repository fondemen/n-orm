package com.mt.storage.conversion;

import com.mt.storage.KeyManagement;
import com.mt.storage.PersistingElement;

class PersistingConverter extends Converter<PersistingElement> {
	public PersistingConverter() {
		super(PersistingElement.class);
	}

	@Override
	public PersistingElement fromString(String rep, Class<?> expected) {
		return (PersistingElement) KeyManagement.aspectOf().createElement(expected, rep);
	}

	@Override
	public String toString(PersistingElement obj) {
		return obj.getIdentifier();
	}

	@Override
	public PersistingElement fromBytes(byte[] rep, Class<?> expected) {
		return this.fromString(ConversionTools.convert(String.class, rep), expected);
	}

	@Override
	public byte[] toBytes(PersistingElement obj) {
		return ConversionTools.convert(this.toString(obj));
	}

	@Override
	public boolean canConvert(Class<?> type) {
		return this.getClazz().isAssignableFrom(type);
	}

}