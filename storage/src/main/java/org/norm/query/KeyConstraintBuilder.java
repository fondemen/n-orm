package org.norm.query;

import java.lang.reflect.Field;

import org.norm.PersistingElement;


public class KeyConstraintBuilder<T extends PersistingElement> {
	private final ClassConstraintBuilder<T> constraintBuilder;
	private final Field key;

	public KeyConstraintBuilder(ClassConstraintBuilder<T> cb, Field key) {
		this.constraintBuilder = cb;
		this.key = key;
	}

	public ClassConstraintBuilder<T> setTo(Object value) {
		if (value == null)
			throw new IllegalArgumentException("Keys cannot be null.");
//		if (! this.key.getType().isAssignableFrom(value.getClass()))
//			throw new IllegalArgumentException("Key " + this.key + " cannot have value of type " + value.getClass());
		this.constraintBuilder.addKeyValue(key, value);
		return this.constraintBuilder;
	}
	
	public RangeKeyConstraintBuilder<T> between(Object value) {
		return new RangeKeyConstraintBuilder<T>(this.constraintBuilder, this.key, value);
	}
	
	public ClassConstraintBuilder<T> greaterOrEqualsThan(Object value) {
		this.constraintBuilder.setSearchedKey(this.key, value, null);
		return this.constraintBuilder;
	}
	
	public ClassConstraintBuilder<T> lessOrEqualsThan(Object value) {
		this.constraintBuilder.setSearchedKey(this.key, null, value);
		return this.constraintBuilder;
	}
}
