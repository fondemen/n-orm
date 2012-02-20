package com.googlecode.n_orm;

import java.util.Date;


public class TimeoutCanceller implements ProcessCanceller {
	private final long duration;
	private long start, end = -1;
	
	public TimeoutCanceller(long timeoutInMs) {
		this.duration = timeoutInMs;
	}
	
	public long getDuration() {
		return duration;
	}

	protected void processStarted() {
		this.start = System.currentTimeMillis();
		this.end = this.start+this.getDuration();
	}
	
	public boolean isCancelled() {
		return this.end == -1 ? false : System.currentTimeMillis()>this.end;
	}
	
	public String getErrorMessage(com.googlecode.n_orm.Process<?> processAction) {
		return "Timeout: process " + processAction.getClass().getName() + ' ' + processAction + " started at " + new Date(start) + " should have finised at " + new Date(end) + " after " + this.getDuration() + "ms but is still running at " + new Date();
	}
}