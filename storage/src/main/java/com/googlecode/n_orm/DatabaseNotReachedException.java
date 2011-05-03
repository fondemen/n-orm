package com.googlecode.n_orm;


/**
 * A problem happened while querying the database. See {@link Throwable#getCause()} for details.
 */
public class DatabaseNotReachedException extends RuntimeException {
	private static final long serialVersionUID = -2698947883988453416L;

	public DatabaseNotReachedException(Exception cause) {
		super("Could not reach database: " + cause, cause);
	}

	public DatabaseNotReachedException(String cause) {
		super("Could not reach database: " + cause);
	}

}
