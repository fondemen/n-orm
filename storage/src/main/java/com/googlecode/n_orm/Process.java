package com.googlecode.n_orm;

import java.io.Serializable;

import com.googlecode.n_orm.query.SearchableClassConstraintBuilder;
import com.googlecode.n_orm.storeapi.ActionnableStore;
import com.googlecode.n_orm.storeapi.Row;


/**
 * The interface for a process to be performed over an element.
 * The main intent of this interface is to be used in conjunction with {@link SearchableClassConstraintBuilder#forEach(Process)},  {@link SearchableClassConstraintBuilder#remoteForEach(Process, Callback, int, long)}, {@link com.googlecode.n_orm.operations.Process#processElements(Class, com.googlecode.n_orm.storeapi.Constraint, Process, int, String[], int, ProcessCanceller, java.util.concurrent.ExecutorService)}, or {@link com.googlecode.n_orm.operations.Process#processElementsRemotely(Class, com.googlecode.n_orm.storeapi.Constraint, Process, Callback, int, String[], int, long)}.
 * A process must have a default constructor (i.e. a constructor with no parameters).
 * @param <Input> The kind of elements to be processed. Should be a {@link PersistingElement} to be used in a search, or a {@link Row} to be used with {@link ActionnableStore}
 */
public interface Process<Input> extends Serializable {
	void process(Input element) throws Throwable;
}