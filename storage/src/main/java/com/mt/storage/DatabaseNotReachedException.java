package com.mt.storage;

import java.util.Map;

/**
 * This exception is thrown when a property (or an element of a {@link Map}) annotated with {@link Incrementing} decrements.
 *
 */
public class DatabaseNotReachedException extends Exception {
	private static final long serialVersionUID = -2698947883988453416L;

	public DatabaseNotReachedException(Exception cause) {
		super("Cound not reach database: " + cause, cause);
	}

}
