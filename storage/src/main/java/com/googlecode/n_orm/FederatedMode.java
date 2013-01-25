package com.googlecode.n_orm;

import com.googlecode.n_orm.storeapi.Store;

/**
 * The different consistency possibilities for a table to be federated.
 * <p>
 * Consistency, for federated tables, consists in avoiding having duplicated
 * rows within two different tables. This can happen depending whether result
 * for the postfix computation (as given by
 * {@link PersistingElementOverFederatedTable#getTablePostfix()}) keep constant
 * with time, that is it always returns the same value for a given persisting
 * element.
 * </p>
 * <p>
 * Three solutions are proposed to check for consistency:
 * <ul>
 * <li>Throwing an {@link IllegalStateException} when postfix computation
 * changes over time (computation consistency)
 * <li>Check existence of identifier for an element in all table alternatives
 * before reading it from data store ; this way, actual table postfix is
 * searched in the data store in preference of calling
 * {@link PersistingElementOverFederatedTable#getTablePostfix()} on read
 * operations
 * <li>Check existence of identifier for an element in all table alternatives
 * before writing it to data store ; this way, actual table postfix is searched
 * in the data store in preference of calling
 * {@link PersistingElementOverFederatedTable#getTablePostfix()} on write
 * operations
 * </ul>
 * </p>
 * <p>
 * Full consistency, be it for read or write, consists in checking existence of
 * the element's identifier in alternative tables. All alternative tables are
 * queried for the existence of the identifier (using
 * {@link Store#exists(com.googlecode.n_orm.storeapi.MetaInformation, String, String)}).
 * Tables are queried in sequence in probability order:
 * <ol>
 * <li>table with computed postfix
 * <li>table with no postfix (a.k.a. original table, i.e. the table in which the
 * element would be located in case it is not using federated tables)
 * <li>alternative tables already known
 * <li>all alternative tables found from the data store as they are saved in a
 * special meta table
 * </ol>
 * </p>
 * <p>
 * Achieving full consistency obviously requires a multiple queries on the data
 * store just for sake of checks. To reduce number of checks, it is possible to
 * restrict consistency checks to the original table, i.e. ignore tables of
 * points 3 and 4. This mode is called "consistency with legacy" as it keeps the
 * table federation consistent when a persisting class is changed from not
 * federated to federated.
 * </p>
 * <p>
 * Enumerated values are named using the following general pattern:
 * <ol>
 * <li>PC when computed postfix should not change over time
 * <li>RCONS (read consistency) or RLEG (read legacy consistency) when read
 * operations trigger consistency checks to determine actual postfix before any
 * read operation (like {@link PersistingElement#activate(String...)} or
 * {@link PersistingElement#existsInStore()})
 * <li>WCONS (write consistency) or WLEG (write legacy consistency) when read
 * operations trigger consistency checks to determine actual postfix before any
 * read operation (like {@link PersistingElement#store()})
 * </ol>
 * When read and write operation should the the same level of consistency
 * checks, only consistency level is indicated. Note that
 * {@link PersistingElement#delete()} uses the most consistent mode between read
 * and write consistency.
 * </p>
 * 
 * @see FederatedTableManagement FederatedTableManagement for more
 *      implementation details
 */
public enum FederatedMode {
	/**
	 * A single table (the original table) is to be used for storing elements.
	 */
	NONE(),
	/**
	 * Table for elements is postfixed by result of
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()}. This can
	 * be dangerous if the latter result changes over time for a given element
	 * as an element can appear in different tables and only one table will be
	 * used for operations.
	 */
	INCONS(false, Consistency.NONE, Consistency.NONE),
	/**
	 * Table for elements is postfixed by result of
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()}. Result for
	 * the latter operation are regularly checked not to change over time.
	 */
	PC_INCONS(true, Consistency.NONE, Consistency.NONE),
	/**
	 * Finds from the datastore whether table for this element is the original
	 * table or table postfixed with
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} prior to
	 * any read operation.
	 */
	RLEG(false, Consistency.CONSISTENT_WITH_LEGACY, Consistency.NONE),
	/**
	 * Finds from the datastore whether table for this element is the original
	 * table or table postfixed with
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} prior to
	 * any read operation. Result for
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} is
	 * regularly checked to be consistent in case table is postfixed.
	 */
	PC_RLEG(true, Consistency.CONSISTENT_WITH_LEGACY, Consistency.NONE),
	/**
	 * Find from the datastore actual table for elements prior to any read
	 * operation before calling
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()}.
	 */
	RCONS(false, Consistency.CONSISTENT, Consistency.NONE),
	/**
	 * Find from the datastore actual table for elements prior to any read
	 * operation before calling
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()}. Result for
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} is
	 * regularly checked to be consistent in case table is postfixed.
	 */
	PC_RCONS(true, Consistency.CONSISTENT, Consistency.NONE),
	/**
	 * Finds from the datastore whether table for this element is the original
	 * table or table postfixed with
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} prior to
	 * any write operation.
	 */
	WLEG(false, Consistency.NONE, Consistency.CONSISTENT_WITH_LEGACY),
	/**
	 * Finds from the datastore whether table for this element is the original
	 * table or table postfixed with
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} prior to
	 * any write operation. Result for
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} is
	 * regularly checked to be consistent in case table is postfixed.
	 */
	PC_WLEG(true, Consistency.NONE, Consistency.CONSISTENT_WITH_LEGACY),
	/**
	 * Finds from the datastore whether table for this element is the original
	 * table or table postfixed with
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} prior to
	 * any read or write operation.
	 */
	LEG(false, Consistency.CONSISTENT_WITH_LEGACY,
			Consistency.CONSISTENT_WITH_LEGACY),
	/**
	 * Finds from the datastore whether table for this element is the original
	 * table or table postfixed with
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} prior to
	 * any write operation. Result for
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} is
	 * regularly checked to be consistent in case table is postfixed.
	 */
	PC_LEG(true, Consistency.CONSISTENT_WITH_LEGACY,
			Consistency.CONSISTENT_WITH_LEGACY),
	/**
	 * Finds from the datastore whether table for this element is the original
	 * table or table postfixed with
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} prior to
	 * any read operation. Regarding write operation, only table with no postfix
	 * is checked.
	 */
	RCONS_WLEG(false, Consistency.CONSISTENT,
			Consistency.CONSISTENT_WITH_LEGACY),
	/**
	 * Finds from the datastore whether table for this element is the original
	 * table or table postfixed with
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} prior to
	 * any read operation. Regarding write operation, only table with no postfix
	 * is checked. Result for
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} is
	 * regularly checked to be consistent in case table is postfixed.
	 */
	PC_RCONS_WLEG(true, Consistency.CONSISTENT,
			Consistency.CONSISTENT_WITH_LEGACY),
	/**
	 * Find from the datastore actual table for elements prior to any write
	 * operation before calling
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()}.
	 */
	WCONS(false, Consistency.NONE, Consistency.CONSISTENT),
	/**
	 * Find from the datastore actual table for elements prior to any write
	 * operation before calling
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()}. Result for
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} is
	 * regularly checked to be consistent in case table is postfixed.
	 */
	PC_WCONS(true, Consistency.NONE, Consistency.CONSISTENT),
	/**
	 * Find from the datastore actual table for elements prior to any write
	 * operation before calling
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()}. Regarding
	 * read operation, only table with no postfix is checked.
	 */
	RLEG_WCONS(false, Consistency.CONSISTENT_WITH_LEGACY,
			Consistency.CONSISTENT),
	/**
	 * Find from the datastore actual table for elements prior to any write
	 * operation before calling
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()}. Regarding
	 * read operation, only table with no postfix is checked. Result for
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} is
	 * regularly checked to be consistent in case table is postfixed.
	 */
	PC_RLEG_WCONS(true, Consistency.CONSISTENT_WITH_LEGACY,
			Consistency.CONSISTENT),
	/**
	 * Find from the datastore actual table for elements prior to any read or
	 * write operation before calling
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()}.
	 */
	CONS(false, Consistency.CONSISTENT, Consistency.CONSISTENT),
	/**
	 * Find from the datastore actual table for elements prior to any read or
	 * write operation before calling
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()}. Result for
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} is
	 * regularly checked to be consistent in case table is postfixed.
	 */
	PC_CONS(true, Consistency.CONSISTENT, Consistency.CONSISTENT), ;

	private final boolean federated;
	private final boolean checkForChangingPostfix;
	private final Consistency readConsistency;
	private final Consistency writeConsistency;

	// Not federated
	FederatedMode() {
		this.federated = false;
		this.checkForChangingPostfix = false;
		this.readConsistency = Consistency.NONE;
		this.writeConsistency = Consistency.NONE;
	}

	// Federated
	FederatedMode(boolean checkForChangingPostfix,
			Consistency readConsistency,
			Consistency writeConsistency) {
		this.federated = true;
		this.checkForChangingPostfix = checkForChangingPostfix;
		this.readConsistency = readConsistency;
		this.writeConsistency = writeConsistency;
	}

	/**
	 * Whether elements of this persisting class might be stored in another able
	 * than that one stated by {@link PersistingMixin#getTable(Class)}
	 */
	public boolean isFederated() {
		return federated;
	}

	/**
	 * Whether elements of this persisting class should be checked for coherent
	 * results for {@link PersistingElementOverFederatedTable#getTablePostfix()}
	 */
	public boolean isCheckForChangingPostfix() {
		return checkForChangingPostfix;
	}

	public Consistency getConsistency(ReadWrite mode) {
		switch (mode) {
		case READ:
			return this.readConsistency;
		case WRITE:
			return this.writeConsistency;
		case READ_OR_WRITE:
			if (this.readConsistency.compareTo(this.writeConsistency) > 0)
				return this.readConsistency;
			else
				return this.writeConsistency;
		}
		throw new Error("Unknown consistency mode " + mode);
	}
}