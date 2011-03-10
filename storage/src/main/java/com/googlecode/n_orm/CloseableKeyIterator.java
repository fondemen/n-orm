package com.googlecode.n_orm;

import java.util.Iterator;

import com.googlecode.n_orm.Constraint;
import com.googlecode.n_orm.Store;

/**
 * An iterator that should be closed once information is read.
 * This is the return type of {@link Store#get(String, Constraint, int)}.
 */
public interface CloseableKeyIterator extends Iterator<String> {

	public abstract void close();

}