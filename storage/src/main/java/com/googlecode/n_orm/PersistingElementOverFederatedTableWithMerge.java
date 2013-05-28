package com.googlecode.n_orm;

/**
 * This interface is to be implemented by persisting classes over
 * {@link Persisting#federated() federated tables}
 * in order to repair any detected inconsistency.
 */
public interface PersistingElementOverFederatedTableWithMerge {// extends PersistingElementOverFederatedTable {
	
	/**
	 * In case an inconsistency is detected, that is when an element
	 * with same id is found in different tables, this method is called in order
	 * to merge the element into one single element.
	 * This element is to be {@link PersistingElement#store() stored} after
	 * this method completes.
	 * Given element is to be deleted after this method completes,
	 * so any important information from elt should be saved into this element.
	 * Throws an exception in case merge is impossible.
	 */
	void mergeWith(PersistingElementOverFederatedTableWithMerge elt) throws Exception;

}
