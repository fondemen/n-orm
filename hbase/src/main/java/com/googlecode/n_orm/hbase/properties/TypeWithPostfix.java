package com.googlecode.n_orm.hbase.properties;

abstract class TypeWithPostfix implements
		Comparable<TypeWithPostfix> {
	private final String postfix;

	public TypeWithPostfix(String postfix) {
		this.postfix = postfix;
	}

	public String getPostfix() {
		return postfix;
	}

	@Override
	public int compareTo(TypeWithPostfix o) {
		if (!this.getClass().equals(o.getClass()))
			return this.getClass().getSimpleName()
					.compareTo(o.getClass().getName());
		return this.getPostfix().compareTo(o.getPostfix());
	}
}