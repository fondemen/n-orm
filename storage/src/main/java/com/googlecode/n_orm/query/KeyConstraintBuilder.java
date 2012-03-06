package com.googlecode.n_orm.query;

import java.lang.reflect.Field;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.consoleannotations.Continuator;

public abstract class KeyConstraintBuilder<T extends PersistingElement> {
	private final ClassConstraintBuilder<T> constraintBuilder;
	private final Field key;

	public KeyConstraintBuilder(ClassConstraintBuilder<T> cb, Field key) {
		this.constraintBuilder = cb;
		this.key = key;
	}

	ClassConstraintBuilder<T> getConstraintBuilder() {
		return constraintBuilder;
	}

	Field getKey() {
		return key;
	}

	protected ClassConstraintBuilder<T> setToInt(Object value) {
		if (value == null)
			throw new IllegalArgumentException("Keys cannot be null.");
//		if (! this.key.getType().isAssignableFrom(value.getClass()))
//			throw new IllegalArgumentException("Key " + this.key + " cannot have value of type " + value.getClass());
		this.constraintBuilder.addKeyValue(key, value);
		return this.constraintBuilder;
	}
	
	protected ClassConstraintBuilder<T> greaterOrEqualsThanInt(Object value) {
		this.constraintBuilder.setSearchedKey(this.key, value, null);
		return this.constraintBuilder;
	}
	
	protected ClassConstraintBuilder<T> lessOrEqualsThanInt(Object value) {
		this.constraintBuilder.setSearchedKey(this.key, null, value);
		return this.constraintBuilder;
	}

	@Continuator
	public InnerClassConstraintBuilder<T> isAnElement() {
		return new InnerClassConstraintBuilder<T>(key, this.constraintBuilder);
	}

}
