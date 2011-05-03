package com.googlecode.n_orm.storeapi;

import java.util.Map;

public interface Row {
	
	String getKey();
	Map<String /* family name */, Map<String /* column qualifier */ , byte[] /* column value */>> getValues();
}
