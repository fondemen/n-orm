package com.googlecode.n_orm;

import java.util.Iterator;

/**
 * An iterator that should be closed once information is read.
 * This is the return type of {@link Store#get(String, Constraint, int)}.
 */
public interface CloseableIterator<T> extends Iterator<T> {

	public abstract void close();
}
