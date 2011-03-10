package org.norm.query;

import java.lang.reflect.Field;

import org.norm.PersistingElement;


public class RangeKeyConstraintBuilder<T extends PersistingElement> {
	private final ClassConstraintBuilder<T> classConstraintBuilder;
	private final Field key;
	private final Object startValue;

	RangeKeyConstraintBuilder(ClassConstraintBuilder<T> constraintBuilder, Field key, Object startValue) {
		this.classConstraintBuilder = constraintBuilder;
		this.key = key;
		this.startValue = startValue;
	}
	
	public ClassConstraintBuilder<T> and(Object includedEndValue) {
		this.classConstraintBuilder.setSearchedKey(key, this.startValue, includedEndValue);
		return this.classConstraintBuilder;
	}

}
