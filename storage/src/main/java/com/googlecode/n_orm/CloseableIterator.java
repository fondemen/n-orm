package com.googlecode.n_orm;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An iterator that should be closed once information is read. This is the
 * return type of
 * {@link com.googlecode.n_orm.storeapi.SimpleStore#get(String, Constraint, int, Set)}
 * .
 * @param T type of the iterated element
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {

	abstract void close();
}
