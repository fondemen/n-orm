package com.googlecode.n_orm;

import java.util.Set;

import com.googlecode.n_orm.cf.ColumnFamily;

public interface PersistingElementListener {

	/**
	 * Sent after a listened {@link PersistingElement} was stored in the data store.
	 * @param pe the listened {@link PersistingElement}
	 */
	void stored(PersistingElement pe);
	
	/**
	 * Sent before a listened {@link PersistingElement} is stored in the data store.
	 * The {@link #stored(PersistingElement)} is invoked afterwards if and only if the persisting element has changed.
	 * @param pe the listened {@link PersistingElement}
	 */
	void storeInvoked(PersistingElement pe);
	
	/**
	 * Sent after a listened {@link PersistingElement} was activated from the data store.
	 * @param pe the listened {@link PersistingElement}
	 * @param activatedColumnFamilies the list of column families that were activated
	 */
	void activated(PersistingElement pe, Set<ColumnFamily<?>> activatedColumnFamilies);
	
	/**
	 * Sent before a listened {@link PersistingElement} was activated from the data store.
	 * It may happen that {@link #activated(PersistingElement, Set)} is not invoked afterwards e.g. in case this activation corresponds to an {@link PersistingElement#activateIfNotAlready(String...)} on an already activated persisting element.
	 * @param pe the listened {@link PersistingElement}
	 * @param columnFamiliesToBeActivated the list of column families that are to be activated
	 */
	void activateInvoked(PersistingElement pe, Set<ColumnFamily<?>> columnFamiliesToBeActivated);
}
