package com.mt.storage;

public class DatabaseNotReachedException extends Exception {
	private static final long serialVersionUID = -2698947883988453416L;

	public DatabaseNotReachedException(Exception cause) {
		super("Cound not reach database: " + cause, cause);
	}

}
