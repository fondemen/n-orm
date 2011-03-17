package com.googlecode.n_orm;

public class UnknownColumnFamily extends RuntimeException {
	private static final long serialVersionUID = -7723779718731879972L;
	
	private final Class<? extends PersistingElement> clazz;
	private final String unknownColumnFamilyName;

	public UnknownColumnFamily(Class<? extends PersistingElement> clazz, String unknownColumnFamilyName) {
		super("Unknnown column family name for " + clazz + ": " + unknownColumnFamilyName);
		this.clazz = clazz;
		this.unknownColumnFamilyName = unknownColumnFamilyName;
	}

	public Class<? extends PersistingElement> getClazz() {
		return clazz;
	}

	public String getUnknownColumnFamilyName() {
		return unknownColumnFamilyName;
	}
}
