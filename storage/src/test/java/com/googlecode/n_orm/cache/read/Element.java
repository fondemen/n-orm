package com.googlecode.n_orm.cache.read;
import java.util.HashMap;
import java.util.Map;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;


@Persisting
public class Element{
	@Key public String key;
	public String familyName;
	public Map<String, Integer> familyData=new HashMap<String,Integer>();
}
