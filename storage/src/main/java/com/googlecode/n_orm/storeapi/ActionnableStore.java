package com.googlecode.n_orm.storeapi;

import java.util.Set;

import com.googlecode.n_orm.Callback;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.Process;

/**
 * A store able to trigger actions server-side.
 */
public interface ActionnableStore extends Store {
	<AE extends PersistingElement, E extends AE> void process(MetaInformation meta, String table, Constraint c, Set<String> families, Class<E> element, Process<AE> action, Callback callback) throws DatabaseNotReachedException;
}
