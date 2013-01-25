package com.googlecode.n_orm;

/**
 * Response of an asynchronous operation once completed. To be used in
 * conjunction with with
 * {@link com.googlecode.n_orm.query.SearchableClassConstraintBuilder#remoteForEach(Process, Callback, int, long)}
 * or
 * {@link com.googlecode.n_orm.operations.Process#processElementsRemotely(Class, com.googlecode.n_orm.storeapi.Constraint, Process, Callback, int, String[], int, long)}
 * .
 */
public interface Callback {

	void processCompleted();

	void processCompletedInError(Throwable error);
}
