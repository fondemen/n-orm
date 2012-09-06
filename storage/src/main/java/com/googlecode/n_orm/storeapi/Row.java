package com.googlecode.n_orm.storeapi;

import java.util.Map;

public interface Row {
	
	public static interface ColumnFamilyData extends
		Map<String /* family name */, 
			Map<String /* column qualifier */ , byte[] /* column value */>>{}
	
	String getKey();
	ColumnFamilyData getValues();
}
