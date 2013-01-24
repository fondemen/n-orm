package com.googlecode.n_orm.utils;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.googlecode.n_orm.CloseableIterator;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Row;

/**
 * An {@link CloseableIterator} composite able to iterate over a set of
 * {@link CloseableIterator}s. Elements are selected from composed iterators
 * so that they are iterated
 * {@link PersistingElement#compareTo(PersistingElement) in an ordered way}.
 */
public class AggregatingIterator implements CloseableKeyIterator {

	/**
	 * A pointer on a composed iterator..
	 */
	private static class IteratorStatus {
		/**
		 * The result of an iterated element. The iterated element is
		 * considered as iterated over only when
		 * {@link ResultReadyToGo#getResult()} is called.
		 */
		private class ResultReadyToGo {

			/**
			 * Get the iterated row and marks it as iterated over, that is
			 * iterator pointed by {@link IteratorStatus} will be called
			 * {@link Iterator#next()} in order to know what is the next
			 * element to be iterated, instead of returning this element
			 * again.
			 * 
			 * @return the iterated row
			 */
			public Row getResult() {
				Row ret = IteratorStatus.this.next;
				IteratorStatus.this.next = null;
				return ret;
			}

			/**
			 * The key of the result. This methods leave the element as the
			 * next element to be iterated.
			 * 
			 * @return
			 */
			private String getKey() {
				return IteratorStatus.this.next.getKey();
			}
		}

		/**
		 * The pointed iterator
		 */
		private final CloseableKeyIterator it;
		/**
		 * The next row to be iterated
		 */
		private Row next = null;
		/**
		 * Whether pointed iterator is empty and closed
		 */
		private boolean done = false;

		public IteratorStatus(CloseableKeyIterator it) {
			this.it = it;
		}

		/**
		 * Grabs the next element to be iterated if no known yet.
		 */
		private void prepareNext() {
			try {
				if (this.done)
					return;
				if (this.next == null) {
					// First call or last result was iterated using
					// ResultReadyToGo.getResult()
					if (this.it.hasNext()) {
						this.next = this.it.next();
					} else {
						// No more elements in this iterator ; closing
						this.close();
					}
				}
			} finally {
				assert this.done == (this.next == null);
			}
		}

		/**
		 * @see CloseableKeyIterator#hasNext()
		 */
		public boolean hasNext() {
			this.prepareNext();
			return this.next != null;
		}

		/**
		 * Returns the next element to be iterated if this element is lower
		 * than parameter. Lower means with a lower key as can be found by
		 * {@link ResultReadyToGo#getKey()}.
		 * 
		 * @param r
		 *            null or a previously iterated key
		 * @return r if it is null or lower than the next element of this
		 *         iterator or the row to be removed from this iterator in
		 *         case {@link ResultReadyToGo#getResult()} is called
		 * @see CloseableKeyIterator#next()
		 */
		public ResultReadyToGo getNextIfLower(ResultReadyToGo r) {
			this.prepareNext();

			if (this.done)
				return r;

			assert this.next != null;

			if (r == null || this.next.getKey().compareTo(r.getKey()) < 0) {
				return new ResultReadyToGo();
			} else {
				return r;
			}
		}

		/**
		 * @see CloseableIterator#close()
		 */
		public void close() {
			if (!this.done) {
				this.it.close();
				this.done = true;
			}
		}
	}

	/**
	 * The composed iterators.
	 */
	private Set<IteratorStatus> status = new LinkedHashSet<IteratorStatus>();
	/**
	 * Whether iteration has started.
	 */
	private boolean started = false;

	/**
	 * Adds an iterator to the list of iterators to be explored.
	 * 
	 * @throws IllegalStateException
	 *             in case the iteration has started using
	 *             {@link #hasNext()} or {@link #next()}.
	 */
	public void addIterator(CloseableKeyIterator it) {
		if (this.started)
			throw new IllegalStateException(
					"Cannot add a new iterator when iteration has started");
		this.status.add(new IteratorStatus(it));
	}

	@Override
	public void close() {
		for (IteratorStatus is : this.status) {
			is.close();
		}
	}

	@Override
	public boolean hasNext() {
		this.started = true;
		for (IteratorStatus is : this.status) {
			if (is.hasNext())
				return true;
		}
		return false;
	}

	@Override
	public Row next() {
		this.started = true;
		IteratorStatus.ResultReadyToGo ret = null;
		for (IteratorStatus is : this.status) {
			ret = is.getNextIfLower(ret);
		}
		return ret.getResult();
	}

	/**
	 * @throws UnsupportedOperationException
	 *             in any case
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
