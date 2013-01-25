package com.googlecode.n_orm;

import java.lang.reflect.Field;

/**
 * Thrown in case a property annotated with {@link Incrementing} is given a lower value than before.
 */
public class IncrementException extends RuntimeException {

	private static final long serialVersionUID = 1180399897249752085L;
	
	private final Field property;
	private final boolean increment;
	private final long delta;

	public IncrementException(Field property, boolean increment, long l) {
		super("Property " + property + " can only " + (increment ? "in" : "de") + "crease with time ; trying to " + (!increment ? "in" : "de") + "crement it of " + Math.abs(l));
		this.property = property;
		this.increment = increment;
		this.delta = l;
	}

	public Field getProperty() {
		return property;
	}

	public boolean isIncrement() {
		return increment;
	}

	public long getDelta() {
		return delta;
	}

}
