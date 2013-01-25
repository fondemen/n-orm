package com.googlecode.n_orm;

public enum ReadWrite {
	READ(true, false), WRITE(false, true), READ_OR_WRITE(true, true);
	private final boolean read;
	private final boolean write;
	private ReadWrite(boolean read, boolean write) {
		this.read = read;
		this.write = write;
	}
	public boolean isRead() {
		return read;
	}
	public boolean isWrite() {
		return write;
	}
}