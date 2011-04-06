package com.googlecode.n_orm.conversion;

abstract class NaturalConverter<T extends Number> extends PrimitiveConverter<T> {

	public NaturalConverter(Class<T> clazz, Class<?> primitiveClazz, int byteSize) {
		super(clazz, primitiveClazz, 1, byteSize, 1, byteSize*2);
	}

	public long parseString(String rep) {
		byte digits = (byte) (rep.length() / 2);
		if (rep.length() % 2 == 1) {
			digits++;
			rep = "0" + rep;
		}
		if (digits == 0)
			return 0;
		byte[] ret = new byte[digits];
		for (int i = 0; i < digits; ++i) {
			char br = rep.charAt(2 * i);
			byte b = 0;
			if (br >= 'a') {
				b = (byte) ((br - (byte) 'a') + (byte) 10);
			} else {
				b = (byte) (br - (byte) '0');
			}
			b <<= 4;
			br = rep.charAt(2 * i + 1);
			if (br >= 'a') {
				b |= (br - (byte) 'a') + (byte) 10;
			} else {
				b |= br - (byte) '0';
			}
			ret[i] = b;
		}
		flipFirstBit(ret);
		return this.parseBytes(ret);
	}

	public String unparseString(long obj) {
		byte[] ret = this.unparseBytes(obj);

		flipFirstBit(ret); // Removing sign so that elements are sorted as
							// expected.

		StringBuffer out = new StringBuffer();
		for (int i = 0; i < ret.length; ++i) {
			byte b = (byte) ((ret[i] >>> 4) & 0x0F);
			if (b < 10)
				out.append((char) (b + (byte) '0'));
			else
				out.append((char) (b - (byte) 10 + (byte) 'a'));
			b = (byte) (ret[i] & 0x0F);
			if (b < 10)
				out.append((char) (b + (int) '0'));
			else
				out.append((char) (b - (byte) 10 + (byte) 'a'));
		}

		return out.toString();
	}

	public long parseBytes(byte[] rep) {
	    long l = 0;
	    for(int i = 0; i < rep.length; i++) {
	      l <<= 8;
	      l ^= (long)rep[i] & 0xFF;
	    }
	    return l;
	}

	public byte[] unparseBytes(long obj) {
	    byte [] b = new byte[this.getMaxByteSize()];
	    for(int i=this.getMaxByteSize()-1;i>0;i--) {
	      b[i] = (byte)(obj);
	      obj >>>= 8;
	    }
	    b[0] = (byte)(obj);
	    return b;
	}

	private void flipFirstBit(byte[] rep) {
		if ((rep[0] & 0x80) == 0)
			rep[0] = (byte) (rep[0] | (byte) 0x80);
		else
			rep[0] = (byte) (rep[0] & (byte) 0x7f);
	}

	@Override
	protected boolean canConvert(byte[] rep) {
		return rep.length <= Long.SIZE/Byte.SIZE;
	}

	@Override
	protected boolean canConvert(String rep) {
		return rep.length()/2 <= Long.SIZE/Byte.SIZE;
	}

}