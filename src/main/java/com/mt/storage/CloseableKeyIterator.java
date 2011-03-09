package com.mt.storage;

import java.util.Iterator;

/**
 * An iterator that should be closed once information is read.
 * This is the return type of {@link Store#get(String, Constraint, int)}.
 */
public interface CloseableKeyIterator extends Iterator<String> {

	public abstract void close();

}