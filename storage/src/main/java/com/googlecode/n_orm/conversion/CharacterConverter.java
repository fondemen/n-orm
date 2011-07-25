package com.googlecode.n_orm.conversion;

class CharacterConverter extends PrimitiveConverter<Character> {
	public static final int BYTE_SIZE = Character.SIZE/Byte.SIZE;
	private final StringBuffer sbuf;

	public CharacterConverter() {
		super(Character.class, char.class, BYTE_SIZE, BYTE_SIZE, 1, 1, Character.MIN_VALUE);
		 sbuf = new StringBuffer(1);
		 sbuf.append(' ');
	}

	@Override
	public Character fromString(String rep, Class<?> expected) {
		return rep.charAt(0);
	}

	@Override
	public String toString(Character obj) {
		sbuf.setCharAt(0, obj.charValue());
		return sbuf.toString();
	}

	@Override
	public Character fromBytes(byte[] rep, Class<?> expected) {
	    char n = 0;
	    n ^= rep[0] & 0xFF;
	    n <<= 8;
	    n ^= rep[1] & 0xFF;
	    return n;
	}

	@Override
	public byte[] toBytes(Character obj) {
	    byte[] b = new byte[BYTE_SIZE];
	    char val = obj.charValue();
	    b[1] = (byte) (val);
	    val >>= 8;
	    b[0] = (byte) (val);
	    return b;
	}

}