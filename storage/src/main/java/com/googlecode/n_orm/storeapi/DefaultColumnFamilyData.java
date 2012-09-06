package com.googlecode.n_orm.storeapi;

import java.util.Map;
import java.util.TreeMap;

import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;

public class DefaultColumnFamilyData extends TreeMap<String /* family name */, 
			Map<String /* column qualifier */ , byte[] /* column value */>> implements
		ColumnFamilyData {

	private static final long serialVersionUID = -7463011284583132839L;
	
	public DefaultColumnFamilyData() {
		super();
	}
	
	public DefaultColumnFamilyData(TreeMap<String, Map<String, byte[]>> clone) {
		super(clone);
	}
	
	public DefaultColumnFamilyData(ColumnFamilyData clone) {
		super(clone);
	}
}
