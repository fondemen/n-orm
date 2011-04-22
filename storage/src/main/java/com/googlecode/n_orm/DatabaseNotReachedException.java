package com.googlecode.n_orm;

import java.util.Map;

import com.googlecode.n_orm.Incrementing;

/**
 * This exception is thrown when a property (or an element of a {@link Map}) annotated with {@link Incrementing} decrements.
 *
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
