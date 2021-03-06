package com.googlecode.n_orm.query;

import java.lang.reflect.Field;

import com.googlecode.n_orm.PersistingElement;


public abstract class RangeKeyConstraintBuilder<T extends PersistingElement> {
	private final ClassConstraintBuilder<T> classConstraintBuilder;
	private final Field key;
	private final Object startValue;

	RangeKeyConstraintBuilder(ClassConstraintBuilder<T> constraintBuilder, Field key, Object startValue) {
		this.classConstraintBuilder = constraintBuilder;
		this.key = key;
		this.startValue = startValue;
	}
	
	protected ClassConstraintBuilder<T> andInt(Object includedEndValue) {
		this.classConstraintBuilder.setSearchedKey(key, this.startValue, includedEndValue);
		return this.classConstraintBuilder;
	}

}
