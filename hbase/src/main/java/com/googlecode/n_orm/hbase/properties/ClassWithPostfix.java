package com.googlecode.n_orm.hbase.properties;

import com.googlecode.n_orm.PersistingElement;

class ClassWithPostfix extends TypeWithPostfix {
	private final Class<? extends PersistingElement> clazz;

	ClassWithPostfix(Class<? extends PersistingElement> clazz,
			String postfix) {
		super(postfix);
		this.clazz = clazz;
	}

	public Class<? extends PersistingElement> getClazz() {
		return clazz;
	}

	@Override
	public int compareTo(TypeWithPostfix o) {
		int ret = super.compareTo(o);
		if (ret != 0)
			return ret;
		return this
				.getClazz()
				.getSimpleName()
				.compareTo(
						((ClassWithPostfix) o).getClazz().getSimpleName());
	}

}