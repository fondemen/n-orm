package com.googlecode.n_orm;

import com.googlecode.n_orm.Incrementing;


/**
 * Thronw in case a property annotated with {@link Incrementing} is given a lower value than before.
 */
public class DecrementException extends RuntimeException {

	private static final long serialVersionUID = 1180399897249752085L;

	public DecrementException(String msg) {
		super(msg);
	}

}
