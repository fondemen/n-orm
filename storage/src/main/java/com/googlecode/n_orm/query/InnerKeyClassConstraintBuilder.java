package com.googlecode.n_orm.query;

import java.lang.reflect.Field;

import com.googlecode.n_orm.Book;
import com.googlecode.n_orm.PersistingElement;

public class InnerKeyClassConstraintBuilder<T extends PersistingElement> extends ClassConstraintBuilder<T> {
	private final ClassConstraintBuilder<T> constraintBuilder;
	private final Field key;

	@SuppressWarnings("unchecked")
	public InnerKeyClassConstraintBuilder(Field key, ClassConstraintBuilder<T> constraintBuilder) {
		super(key.getType());
		this.constraintBuilder = constraintBuilder;
		this.key = key;
	}

	@Override
	public KeyConstraintBuilder<T> createKeyBuilder(
			Field f) {
		return new InnerKeyConstraintBuilder<T>(this, f);
	}

	public SearchableClassConstraintBuilder<T> and() {
		this.constraintBuilder.setSubConstraint(key, this.getConstraint());
		if (this.constraintBuilder instanceof SearchableClassConstraintBuilder)
			return (SearchableClassConstraintBuilder<T>)this.constraintBuilder;
		else if (this.constraintBuilder instanceof InnerKeyClassConstraintBuilder)
			return ((InnerKeyClassConstraintBuilder<T>)this.constraintBuilder).and();
		else
			assert false;
		return null;
	}

	public InnerKeyConstraintBuilder<T> withKey(String key) {
		return (InnerKeyConstraintBuilder<T>) super.withKeyInt(key);
	}
}
