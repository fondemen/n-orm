package com.googlecode.n_orm.cache;

import java.util.ConcurrentModificationException;
import java.util.Iterator;

public class LIFO<T> implements Iterable<T> {
	private static class Node<T> {
		private T element;
		private Node<T> head;
		private Node<T> tail;
		
		public Node(T top, Node<T> tail, Node<T> owner) {
			super();
			this.element = top;
			this.tail = tail;
			this.head = owner;
		}
	}
	
	public static interface Criterium {
		boolean check(Node<?> node, Object... args);
	}
	
	private static final Criterium TAIL_CRITERIUM = new Criterium() {

		@Override
		public boolean check(Node<?> node, Object... args) {
			return node.tail == null;
		}
		
	};
	
	private static final Criterium CONTAINS_CRITERIUM = new Criterium() {

		@Override
		public boolean check(Node<?> node, Object... args) {
			return node.element.equals(args[0]);
		}
		
	};
	
	private class LIFOIterator implements Iterator<T> {
		private Node<T> next;
		private Node<T> previous;
		boolean removed = false;
		
		public LIFOIterator() {
			this.next = getFirstElement();
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public T next() {
			if (this.next == null)
				throw new IllegalStateException("Iterator is empty");
			T ret = this.next.element;
			this.previous = this.next;
			this.next = this.next.head;
			this.removed = false;
			return ret;
		}

		@Override
		public void remove() {
			if (removed)
				return;
			if (previous == null)
				throw new IllegalStateException("Cannot remove before calling next()");
			synchronized(LIFO.this) { //Can't do pop at same time...
				if (actualList == this.previous) {
					pop();
					this.previous = null;
				} else if (this.previous.head == null) { //We've been too far (some pops happened since last next)
					this.previous = null;
				} else {
					Node<T> prev = this.previous.head;
					Node<T> next = this.previous.tail;
					next.head = prev;
					prev.tail = next;
				}
				this.removed = true;
			}
		}
		
	}
	
	private Node<T> actualList;
	
	public synchronized T pop() {
		if (actualList == null)
			return null;
		T ret = actualList.element;
		if (actualList.tail == null) {
			actualList = null;
		} else {
			this.actualList.tail.head = null;
			this.actualList = this.actualList.tail;
		}
		return ret;
	}
	
	public synchronized void push(T element) {
		if (element == null)
			return;
		Node<T> nextActualList = new Node<T>(element, this.actualList, null);
		if (this.actualList != null)
			this.actualList.head = nextActualList;
		this.actualList = nextActualList;
	}

	public synchronized void pushAll(LIFO<T> available) {
		Node<T> fa = available.getFirstElement();
		if (fa == null)
			return;
		assert fa.tail == null;
		fa.tail = this.actualList;
		if (this.actualList != null)
			this.actualList.head = fa;
		this.actualList = fa;
	}

	/**
	 * An iterator starting from the tail of the LIFO.
	 * Supports {@link Iterator#remove()}.
	 * The iterator should be used by only one thread.
	 * Won't throw {@link ConcurrentModificationException} even if LIFO is modified (for instance by calling {@link #push(Object)}, {@link Iterator#remove()}}.
	 * However, it can iterate over popped or removed elements...
	 */
	@Override
	public Iterator<T> iterator() {
		return new LIFOIterator();
	}

	public void clear() {
		this.actualList = null;
	}

	public Node<T> findWithCriterium(Criterium c, Object... args) {
		Node<T> ret = this.actualList;
		while (ret != null) {
			if (c.check(ret, args))
				return ret;
			ret = ret.tail;
		}
		return null;
	}
	
	public Node<T> getFirstElement() {
		Node<T> ret = this.findWithCriterium(TAIL_CRITERIUM);
		assert ret == null || ret.tail == null;
		return ret;
	}

	public boolean contains(T element) {
		return this.findWithCriterium(CONTAINS_CRITERIUM, element) != null;
	}
}