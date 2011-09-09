package com.googlecode.n_orm;

import com.googlecode.n_orm.query.SearchableClassConstraintBuilder;

/**
 * Response af an asynchronous operation once completed.
 * To be used in cunjunction with with {@link SearchableClassConstraintBuilder#remoteForEach(Process)} or {@link StorageManagement#processElementsRemotely(Class, com.googlecode.n_orm.storeapi.Constraint, Process, int, String...)}.
 * @author fondemen
 *
 */
public interface Callback {

	void processCompleted();
	void processCompletedInError(Throwable error);
}
