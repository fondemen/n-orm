package com.googlecode.n_orm;

/**
 * A {@link Callback} implementation that waits for a process to be completed.
 * Just run your process with this call-back, and call {@link #waitProcessCompleted()}.
 * @author fondemen
 *
 */
public class WaitingCallBack implements Callback {
	 private Throwable error = null;
	 private volatile boolean done = false;

	public Throwable getError() {
		return error;
	}

	@Override
	public synchronized void processCompleted() {
		this.done = true;
		this.notify();
	}

	@Override
	public synchronized void processCompletedInError(Throwable error) {
		this.done = true;
		this.error = error;
		this.notify();
	}
	
	public synchronized void waitProcessCompleted() throws InterruptedException {
		if (this.done)
			return;
		this.wait();
	}
	 
}
