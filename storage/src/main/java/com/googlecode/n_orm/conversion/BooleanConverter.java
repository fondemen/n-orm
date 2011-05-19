package com.googlecode.n_orm.conversion;

class BooleanConverter extends PrimitiveConverter<Boolean> {
	public static final String TRUE = "1";
	public static final String FALSE = "0";
	public BooleanConverter() {
		super(Boolean.class, boolean.class, 1, 1, 1, 1, Boolean.FALSE);
	}

	@Override
	public Boolean fromString(String rep, Class<?> expected) {
		if (rep.equals(TRUE))
			return Boolean.TRUE;
		else if (rep.equals(FALSE))
			return Boolean.FALSE;

		throw new IllegalArgumentException("Cannot convert " + rep + " to a boolean.");
	}

	@Override
	public String toString(Boolean obj) {
		return obj.booleanValue() ? TRUE : FALSE;
	}

	@Override
	protected boolean canConvert(byte[] obj) {
		return obj.length == 1;
	}

	@Override
	public Boolean fromBytes(byte[] rep, Class<?> expected) {
	      return rep[0] != (byte)0;
	}

	@Override
	public byte[] toBytes(Boolean obj) {
	    byte [] rep = new byte[1];
	    rep[0] = obj.booleanValue() ? (byte)-1: (byte)0;
	    return rep;
	}

	@Override
	public Boolean revert(Boolean obj) {
		return obj ? Boolean.FALSE : Boolean.TRUE;
	}

}