package com.googlecode.n_orm.hbase.properties;

import java.lang.reflect.Field;

import com.googlecode.n_orm.PersistingElement;

// A class to be used as the key for properties cache
class PropertyCacheKey {
	private final int hashCode;
	private final Class<? extends PersistingElement> clazz;
	private final Field field;
	private final String postfix;

	PropertyCacheKey(Class<? extends PersistingElement> clazz,
			Field field, String postfix) {
		super();
		this.clazz = clazz;
		this.field = field;
		this.postfix = postfix;
		final int prime = 31;
		int result = 1;
		result = prime * result + ((clazz == null) ? 0 : clazz.hashCode());
		result = prime * result + ((field == null) ? 0 : field.hashCode());
		result = prime * result
				+ ((postfix == null) ? 0 : postfix.hashCode());
		this.hashCode = result;
	}

	@Override
	public int hashCode() {
		return this.hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PropertyCacheKey other = (PropertyCacheKey) obj;
		if (hashCode != other.hashCode)
			return false;
		if (clazz == null) {
			if (other.clazz != null)
				return false;
		} else if (!clazz.equals(other.clazz))
			return false;
		if (field == null) {
			if (other.field != null)
				return false;
		} else if (!field.equals(other.field))
			return false;
		if (postfix == null) {
			if (other.postfix != null)
				return false;
		} else if (!postfix.equals(other.postfix))
			return false;
		return true;
	}
}