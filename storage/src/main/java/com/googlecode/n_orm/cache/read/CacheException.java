package com.googlecode.n_orm.cache.read;

public class CacheException extends Exception {

	private static final long serialVersionUID = 3013685703470522352L;

	public CacheException() {
	}

	public CacheException(String message) {
		super(message);
	}

	public CacheException(Throwable cause) {
		super(cause);
	}

	public CacheException(String message, Throwable cause) {
		super(message, cause);
	}

}
