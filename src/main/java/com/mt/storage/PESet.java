package com.mt.storage;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PESet<T extends PersistingElement> extends AbstractSet<T> implements Set<T> {
	private Map<String, T> elements = new TreeMap<String, T>();

	@Override
	public int size() {
		return this.elements.size();
	}

	@Override
	public boolean isEmpty() {
		return this.elements.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return (o instanceof PersistingElement) && this.elements.containsKey(((PersistingElement)o).getIdentifier());
	}

	@Override
	public Iterator<T> iterator() {
		return this.elements.values().iterator();
	}

	@Override
	public Object[] toArray() {
		return this.elements.values().toArray();
	}

	@Override
	public <U> U[] toArray(U[] a) {
		return this.elements.values().toArray(a);
	}

	@Override
	public boolean add(T e) {
		return this.elements.put(e.getIdentifier(), e) == null;
	}

	@Override
	public boolean remove(Object o) {
		if (! (o instanceof PersistingElement))
			return false;
		return this.elements.remove(((PersistingElement)o).getIdentifier()) != null;
	}

	@Override
	public void clear() {
		this.elements.clear();
	}

}
