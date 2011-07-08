package com.googlecode.n_orm;

import com.googlecode.n_orm.storeapi.Row;

public final class EmptyCloseableIterator implements
		com.googlecode.n_orm.storeapi.CloseableKeyIterator {
	@Override
	public void close() {
	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public Row next() {
		return null;
	}

	@Override
	public void remove() {
	}
}