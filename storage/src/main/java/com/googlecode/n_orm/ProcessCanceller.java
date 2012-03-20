package com.googlecode.n_orm;

public interface ProcessCanceller {
	public boolean isCancelled();
	public String getErrorMessage(com.googlecode.n_orm.Process<?> processAction);
}