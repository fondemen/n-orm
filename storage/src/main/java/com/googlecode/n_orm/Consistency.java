package com.googlecode.n_orm;

public enum Consistency {
	// Values should be kept in importance order (a value implies
	// previous values)
	NONE, CONSISTENT_WITH_LEGACY, CONSISTENT;
	public static Consistency fromInt(final byte number) {
		switch (number) {
		case 0:
			return NONE;
		case 1:
			return CONSISTENT_WITH_LEGACY;
		case 2:
			return CONSISTENT;
		default:
			throw new IllegalArgumentException();
		}
	}
}
