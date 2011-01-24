package com.mt.storage;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mt.storage.conversion.ConversionTools;

public class Constraint {
	
	protected static Map<Field, Object> toMapOfFields(Class<?> clazz, Map<String, Object> values) {
		if (values == null)
			return null;
		
		Map<String, Field> fields = new HashMap<String, Field>();
		for (Field f : PropertyManagement.getInstance().getProperties(clazz)) {
			fields.put(f.getName(), f);
		}
		
		Map<Field, Object> ret = new HashMap<Field, Object>();
		for (String field : values.keySet()) {
			ret.put(fields.get(field), values.get(field));
		}
		
		return ret;
	}
	
	private final String startKey, endKey;
	
	public Constraint(String startKey, String endKey) {
		if (startKey == null && endKey == null)
			throw new IllegalArgumentException("A search requires at least a start or an end value.");
		this.startKey = startKey;
		this.endKey = endKey;
	}
	
	/**
	 * Describes a search for a particular key.
	 * To search for a range of a key of cardinality n, all values for k of cardinalities less than n must be supplied.
	 * startValue are endValue both inclusive.
	 * @param checkKeys to check whether all keys with a lower category is given a value
	 */
	Constraint(Map<Field, Object> values, Field searchedKey, Object startValue, Object endValue, boolean checkKeys) {
		if (startValue == null && endValue == null)
			throw new IllegalArgumentException("A search requires at least a start or an end value.");
		
		if (searchedKey == null)
			throw new IllegalArgumentException("A search can only happen on a key ; please, supply one.");
		
		Class<?> clazz = searchedKey.getDeclaringClass();
		
		List<Field> keys = KeyManagement.getInstance().detectKeys(clazz);
		int length = values == null ? 0 : values.size();
		if (keys.size() < length)
			throw new IllegalArgumentException("Too many constrained values compared to the number of keys ; only key values may be constrained.");
		StringBuffer fixedPartb = new StringBuffer();
		String sep = KeyManagement.getInstance().getSeparator(clazz);
		Field f; Object val;
		values = values == null ? new HashMap<Field, Object>() : new HashMap<Field, Object>(values);
		for(int i = 0; i < length; ++i) {
			f = keys.get(i);
			val = values.get(f);
			if (val == null) {
				if (checkKeys)
					throw new IllegalArgumentException("In order to select an element depending on " + searchedKey + ", you must supply a value for " + f);
			} else {
				fixedPartb.append(ConversionTools.convertToString(values.get(keys.get(i))));
				fixedPartb.append(sep);
				values.remove(f);
			}
		}
		if (!values.isEmpty())
			throw new IllegalArgumentException("Can only search according to keys with cardinality up to the search key (here " + searchedKey + ") ; remove values for " + values.keySet());
		
		String fixedPart = fixedPartb.length() > 0 ? fixedPartb.toString() : null;
		if(startValue == null) {
			if (fixedPart == null) {
				this.startKey = null;
			} else {
				this.startKey = fixedPart;
			}
		} else
			this.startKey = (fixedPart == null ? "" : fixedPart) + ConversionTools.convertToString(startValue);

		char sepp = (char) (sep.charAt(sep.length()-1) + 1);
		sep = sep.substring(0, sep.length()-1) + sepp;
		if (endValue == null) {
			if (fixedPart == null) {
				this.endKey = null;
			} else {
				this.endKey = fixedPart + sep;
			}
		} else
			this.endKey = (fixedPart == null ? "" : fixedPart) + ConversionTools.convertToString(endValue) + sep;
	}

	/**
	 * Describes a search for a particular key.
	 * To search for a range of a key of cardinality n, all values for k of cardinalities less than n must be supplied.
	 * startValue are endValue both inclusive.
	 */
	public Constraint(Class<?> type, Map<String, Object> values, String field, Object startValue, Object endValue) {
		this(toMapOfFields(type, values), PropertyManagement.getInstance().getProperty(type, field), startValue, endValue, true);
	}

	public String getStartKey() {
		return startKey;
	}
	
	public String getEndKey() {
		return endKey;
	}
	
}
