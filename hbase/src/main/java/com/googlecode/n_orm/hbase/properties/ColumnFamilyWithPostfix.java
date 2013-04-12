package com.googlecode.n_orm.hbase.properties;

import java.lang.reflect.Field;


class ColumnFamilyWithPostfix extends TypeWithPostfix {
	private final Field field;

	ColumnFamilyWithPostfix(Field field, String postfix) {
		super(postfix);
		this.field = field;
	}

	public Field getField() {
		return field;
	}

	@Override
	public int compareTo(TypeWithPostfix o) {
		int ret = super.compareTo(o);
		if (ret != 0)
			return ret;
		return this
				.getField()
				.getName()
				.compareTo(
						((ColumnFamilyWithPostfix) o).getField().getName());
	}

}