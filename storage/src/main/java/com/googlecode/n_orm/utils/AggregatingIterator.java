package com.googlecode.n_orm.utils;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.googlecode.n_orm.CloseableIterator;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PersistingMixin;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Row;

/**
 * An {@link CloseableIterator} composite able to iterate over a set of
 * {@link CloseableIterator}s. Elements are selected from composed iterators
 * so that they are iterated
 * {@link PersistingElement#compareTo(PersistingElement) in an ordered way}.
 * In case an element with same id is found from different iterators, an
 * {@link IllegaleStateException exception} is thrown, unless
 * {@link #merge(Row, CloseableKeyIterator, Row, CloseableKeyIterator)}
 * is overridden.
 */
public class AggregatingIterator implements CloseableKeyIterator {

	/**
	 * A pointer on a composed iterator..
	 */
	private class IteratorStatus {
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
			
			/**
			 * The {@link IteratorStatus} to which this result belongs
			 * @return
			 */
			private IteratorStatus getIteratorStatus() {
				return IteratorStatus.this;
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

			if (r == null) {
				return new ResultReadyToGo();
			} else {
				int cmp = this.next.getKey().compareTo(r.getKey());
				if (cmp == 0) {
					try {
						this.next = AggregatingIterator.this.merge(this.next, this.it, r.getResult(), r.getIteratorStatus().it);
					} catch (RuntimeException x) {
						throw x;
					} catch (Exception x) {
						throw new RuntimeException(x);
					}
					r.getIteratorStatus().next = null;
					return new ResultReadyToGo();
				} else if (cmp < 0) {
					return new ResultReadyToGo();
				} else /*if (cmp > 0)*/ {
					return r;
				}
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
	
	public Row merge(Row r1, CloseableKeyIterator it1, Row r2, CloseableKeyIterator it2) throws Exception {
		throw new IllegalStateException("Found element with same key "
				+ PersistingMixin.getInstance().identifierToString(r1.getKey())
				+ " from different iterators: "
				+ it1.toString()
				+ " and " + it2.toString());
	}
}
