package com.googlecode.n_orm.query;

import java.lang.reflect.Field;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.consoleannotations.Continuator;

public class InnerClassConstraintBuilder<T extends PersistingElement> extends ClassConstraintBuilder<T> {
	private final ClassConstraintBuilder<T> constraintBuilder;
	private final Field key;

	@SuppressWarnings("unchecked")
	public InnerClassConstraintBuilder(Field key, ClassConstraintBuilder<T> constraintBuilder) {
		super(key.getType());
		this.constraintBuilder = constraintBuilder;
		this.key = key;
	}

	@Override
	public KeyConstraintBuilder<T> createKeyBuilder(
			Field f) {
		return new InnerKeyConstraintBuilder<T>(this, f);
	}

	@SuppressWarnings("unchecked")
	@Continuator
	public SearchableClassConstraintBuilder<T> and() {
		this.constraintBuilder.setSubConstraint(key, this.getConstraint());
		if (this.constraintBuilder instanceof SearchableClassConstraintBuilder)
			return (SearchableClassConstraintBuilder<T>)this.constraintBuilder;
		else if (this.constraintBuilder instanceof InnerClassConstraintBuilder)
			return ((InnerClassConstraintBuilder<T>)this.constraintBuilder).and();
		else
			assert false;
		return null;
	}

	@Continuator
	public InnerKeyConstraintBuilder<T> withKey(String key) {
		return (InnerKeyConstraintBuilder<T>) super.withKeyInt(key);
	}

	@Continuator
	public InnerKeyConstraintBuilder<T> andWithKey(String key) {
		return this.withKey(key);
	}
}
