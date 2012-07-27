package com.googlecode.n_orm.memory;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.googlecode.n_orm.memory.Memory.Table.Row;

/**
 * A queue responsible for evicting expired row from the {@link Memory memory store}.
 * Implementation is based on a {@link DelayQueue}.
 * Listeners can be informed after an eviction was performed on a row using {@link #addEvictionListener(EvictionListener)}.
 * Starting an eviction queue actually starts a {@link Runtime#addShutdownHook(Thread) shutdown hook} for evicting all elements
 * of the store that were planned for eviction (though there is no guarantee that all elements are actually evicted).
 * As such, listeners are all told an eviction happened on each evictable row.
 */
public class EvictionQueue {
	
	/**
	 * The elements to be set in the {@link DelayQueue}.
	 */
	private static class DelayedRow implements Delayed {
		
		/**
		 * The row that will eventually expire
		 */
		private final Row row;

		/**
		 * The expiration date at which {@link #row the row} should expire
		 */
		private final long evictionDateNano;
		
		private DelayedRow(Row row, long evictionDateMs) {
			super();
			this.row = row;
			this.evictionDateNano = TimeUnit.NANOSECONDS.convert(evictionDateMs, TimeUnit.MILLISECONDS);
		}

		@Override
		public int compareTo(Delayed d) {
			if (this == d)
				return 0;
			DelayedRow o = (DelayedRow)d;
			if (this.evictionDateNano == o.evictionDateNano)
				return row.getKey().compareTo(o.row.getKey());
			else if (this.evictionDateNano < o.evictionDateNano)
				return -1;
			else //if (this.evictionDate > o.evictionDate)
				return 1;
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return unit.convert(this.evictionDateNano-System.nanoTime(), TimeUnit.NANOSECONDS);
		}
		
	}
	
	/**
	 * The thread responsible for row eviction of the {@link Memory memory store}.
	 * This thread is the only allowed to  call the eviction method {@link Row#evict()}.
	 * In order for listeners 
	 */
	class EvictionThread extends Thread {
		private volatile boolean running = true;
		
		@Override
		public synchronized void start() {
			super.start();
			// Registering last chance to evict all the list of elements in the store
			// Must extends EvictionThread in order to be allowed to call Row#evict
			Runtime.getRuntime().addShutdownHook(new EvictionThread() {

				@Override
				public void run() {
					try {
						shutdown();
						cleanAll();
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
				
			});
		}

		/**
		 * Stops this thread.
		 */
		private void shutdown() {
			this.running = false;
			this.interrupt();
		}

		@Override
		public void run() {
			while (this.running) {
				try {
					DelayedRow r = queue.take();
					evict(r);
				} catch (InterruptedException e) {
				}
			}
		}

		/**
		 * Removes an outdated row from its store and informs listeners.
		 * Waits for any update on this row to be completed (see {@link Row#evict()}.
		 * @param r the row to be evicted.
		 */
		private void evict(DelayedRow r) throws InterruptedException {
			r.row.evict();
			for (EvictionListener l : listeners) {
				l.rowEvicted(r.row);
			}
		}

		/**
		 * Best effort to evict all planned rows, <u>including not outdated ones</u> !
		 * @throws InterruptedException
		 */
		private void cleanAll() throws InterruptedException {
			// Cleaning queue
			Iterator<DelayedRow> it = queue.iterator();
			while(it.hasNext()) {
				evict(it.next());
				it.remove();
			}
		}
		
	}
	
	/**
	 * Whether this eviction queue was started.
	 */
	private volatile AtomicBoolean started = new AtomicBoolean(false);
	
	/**
	 * The actual queue.
	 */
	private final DelayQueue<DelayedRow> queue = new DelayQueue<DelayedRow>();
	
	/**
	 * The thread waiting on the {@link #queue} for elements to evict.
	 */
	private final EvictionThread evictionThread = new EvictionThread();
	
	/**
	 * The set of listeners to be informed of an eviction.
	 */
	private final Collection<EvictionListener> listeners = Collections.synchronizedCollection(new LinkedList<EvictionListener>());
	
	/**
	 * Starts the eviction thread if not already started.
	 * This method has to be explicitly called after object is constructed.
	 */
	void start() {
		if (this.started.compareAndSet(false, true))
			this.evictionThread.start();
	}

	/**
	 * Registers a row to be evicted.
	 * @param evictionDate the date at which this row has to be evicted.
	 * @param row the row to be evicted
	 */
	public void put(long evictionDate, Row row) {
		this.queue.put(new DelayedRow(row, evictionDate));
	}
	
	/**
	 * Registers an eviction listener to be informed of all eviction on this queue.
	 * @param l the listener
	 */
	public void addEvictionListener(EvictionListener l) {
		this.listeners.add(l);
	}

}
