package org.norm.conversion;

import java.io.UnsupportedEncodingException;

class StringConverter extends SimpleConverter<String> {
	public StringConverter() {
		super(String.class);
	}

	@Override
	public String fromString(String rep, Class<?> expected) {
		return rep;
	}

	@Override
	public String toString(String obj) {
		return obj;
	}

	@Override
	public String fromBytes(byte[] rep, Class<?> expected) {
		if (rep == null) {
			return null;
		}
		String result = null;
		try {
			result = new String(rep, 0, rep.length, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public byte[] toBytes(String obj) {
		if (obj == null) {
			return null;
		}
		byte[] result = null;
		try {
			result = obj.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return result;
	}

}