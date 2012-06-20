package com.googlecode.n_orm;

/**
 * Such kind of persisting elements are those that were annotated with
 * {@link Persisting#federated()} with a mode different from
 * {@link Persisting.FederatedMode#NONE}. Unlike other persisting elements,
 * those elements can be located in more than one table. Actual table can be the
 * table for other elements, or any other table with the same name postfixed
 * with a qualifier. In case this qualifier is not known,
 * {@link #getTablePostfix()} is called so that it is determined ; as such this
 * method must be implemented by such annotated classes.
 */
public interface PersistingElementOverFederatedTable extends PersistingElement {

	/**
	 * Can be called at any time to determine the postfix for the storage table.
	 * Null value is considered equivalent to no postfix, i.e. the "" string.
	 * Try to make this method return always the same value for a given object,
	 * e.g. by relying only on keys.
	 * 
	 * @see Persisting#federated()
	 */
	String getTablePostfix();
}
