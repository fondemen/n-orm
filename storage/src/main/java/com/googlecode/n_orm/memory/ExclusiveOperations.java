package com.googlecode.n_orm.memory;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Synchronization class that avoids two different operations to be executed at same time,
 * while different processes might run concurrently the same operation.
 * An operation is given by its {@link byte} id.
 * Starting an operation is performed by {@link #start(byte)} while {@link #done(byte)} declares that the operation is completed.
 * <br>
 * Example usage:<br><code>
 * public void read() {<br>
 * &nbsp;&nbsp;exclusiveOp.softenedStart(1);<br>
 * &nbsp;&nbsp;try {<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;... //Reads data while it's not written<br>
 * &nbsp;&nbsp;} finally {<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;exclusiveOp.done(1);<br>
 * &nbsp;&nbsp;}<br>
 * }<br>
 * <br>
 * public void update() {<br>
 * &nbsp;&nbsp;exclusiveOp.softenedStart(2);<br>
 * &nbsp;&nbsp;try {<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;... //Writes data while it's not read<br>
 * &nbsp;&nbsp;} finally {<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;exclusiveOp.done(2);<br>
 * &nbsp;&nbsp;}<br>
 * }<br>
 * <br>
 * </code>
 */
public class ExclusiveOperations {
	//List of (blocked ?) elements attempting to perform the same operation
	public static class Blocker {
		private final byte operation; //The operation for the blocker
		private final AtomicBoolean blocking; //Whether or not blocker should block
		private final AtomicLong enteredProcesses = new AtomicLong(0);
		private volatile long waitingProcesses = 0;
		private final AtomicReference<Blocker> next = new AtomicReference<Blocker>();
		private volatile boolean done = false; //Whether this blocker can be used
		
		//private final ConcurrentSkipListSet<Long> threads = new ConcurrentSkipListSet<Long>();
		
		private Blocker(byte operation, boolean blocking) {
			this.operation = operation;
			this.blocking = new AtomicBoolean(blocking);
		}
		
		public byte getOperation() {
			return this.operation;
		}
		
		private boolean isDone() {
			return done;
		}
		
		private Blocker getNext() {
			return this.next.get();
		}
		
		private boolean setNext(Blocker blocker) {
			return this.next.compareAndSet(null, blocker);
		}
		
		private boolean enter() throws InterruptedException {
			long g = this.enteredProcesses.getAndIncrement();
			assert g >= 0;
			if (blocking.get()) {
				synchronized(this) {
					if (this.done)
						return false;
					if (!blocking.get()) //We've been released before entering the synchronized section
						return true;
					this.waitingProcesses++;
					this.wait();
					this.waitingProcesses--;
					assert ! blocking.get();
					return true;
				}
			}
			return !done;
		}
		
		/**
		 * Signals that a process has finished.
		 * Returns true if it's the last process expected to leave
		 */
		public void leave() {
			assert blocking.get() == false;
			
			if (this.enteredProcesses.decrementAndGet() == 0) { //We're the last expected to leave
				Blocker toBeReleased = null;
				synchronized(this) {
					this.done = true;
					if (this.waitingProcesses > 0) {
						this.notifyAll();
					} else {
						if (this.enteredProcesses.get() == 0) { //No other process can be accepted when done is set to true...
							Blocker next = this.next.getAndSet(null); //Release must be called once
							if (next != null)
								toBeReleased = next; //Releasing outside this synchronized section
						} //else another process P entered ; it'll be up to P to release next blocker
					}
				}
				if (toBeReleased != null) {
					toBeReleased.release();
				}
			}
			
		}
		
		private void release() {
			if (this.blocking.compareAndSet(true, false)) {
				if (this.enteredProcesses.get() == 0) { //It seems that no process is waiting for this block...
					this.done = true;
					if (this.enteredProcesses.get() == 0) { //No other process can be accepted when done is set to true...
						Blocker next = this.next.getAndSet(null); //Release must be called once
						if (next != null) {
							next.release();
							return;
						}
					}
				}
				synchronized (this) {
					assert waitingProcesses > 0;
					if (this.waitingProcesses > 3) {
						System.out.println("Releasing " + this.waitingProcesses + " processes for operation " + operation);
					}
					this.notifyAll();
				}
			} else {
				assert false;
			}
		}
	}
	
	/**
	 * The top blocker
	 */
	private final AtomicReference<Blocker> last = new AtomicReference<Blocker>();
	private final Object lastUpgradeMutex = new Object();
	
	/**
	 * Declaration for a process that it starts an operation ; once done, you MUST invoke {@link Blocker#leave()} on the returned object.
	 * Many processes can execute a same operation, but not while another operation is being executed.
	 * In the latter case, the process is blocked until other operation are ended.
	 * In order to be fair, in case some process/es is/are waiting, any entering process will be blocked.
	 * @param operation the id of the operation
	 * @throws InterruptedException in case a process is interrupted
	 */
	public Blocker start(byte operation) throws InterruptedException {
		while (true) {
			//Let's find the end of the list
			Blocker blocker = new Blocker(operation, false); //First blocker never blocks
			if (last.compareAndSet(null, blocker)) {
				//We're the very first process there...
			} else {
				//A list already exists
				blocker = last.get();
			}

			if (blocker.isDone() || blocker.getOperation() != operation) {
				//We should create a blocker for us and append it to the end of the list ; blocker must not be blocking if previous (last) is
				Blocker newBlocker = new Blocker(operation, !blocker.isDone());
				if (blocker.setNext(newBlocker)) { //Try to put newBlock at the end of the list
					//We won ; it's up to us to update last
					Blocker previousLast = last.getAndSet(newBlocker);
					assert previousLast == blocker;
					blocker = newBlocker;
				} else { //Another process already set itself at the end of the list or last was removed by a concurrent done
					blocker = null;
				}
			}
		
			if (blocker != null) {
				assert blocker.getOperation() == operation;
				if (blocker.enter()) { //Fails if done
					return blocker;
				}
			}
			
			//Failed to enter; retrying...
			Thread.yield();
		}

	}
	
	/**
	 * A version of {@link #start(byte)} that throws a {@link RuntimeException} instead of a {@link InterruptedException}.
	 * @return 
	 */
	public Blocker softenedStart(byte operation) {
		try {
			return this.start(operation);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
