package com.googlecode.n_orm;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Set;

/**
 * An iterator that should be closed once information is read.
 * This is the return type of {@link com.googlecode.n_orm.storeapi.SimpleStore#get(String, Constraint, int, Set)}.
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {

	public abstract void close();
}
