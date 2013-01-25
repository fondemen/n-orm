package com.googlecode.n_orm.conversion;

public class UnreversibleTypeException extends IllegalArgumentException {

	private static final long serialVersionUID = -2239097319110076857L;
	
	private final Class<?> type;

	public UnreversibleTypeException(Class<?> type) {
		super("Cannot reverse type " + type);
		this.type = type;
	}

	public UnreversibleTypeException(String message, Class<?> type, Throwable cause) {
		super(message + " (cannot reverse type " + type + ")", cause);
		this.type = type;
	}

	public UnreversibleTypeException(String message, Class<?> type) {
		super(message + " (cannot reverse type " + type + ")");
		this.type = type;
	}

	public UnreversibleTypeException(Class<?> type, Throwable cause) {
		super("Cannot reverse type " + type, cause);
		this.type = type;
	}

	public Class<?> getType() {
		return type;
	}

}
