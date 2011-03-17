package com.googlecode.n_orm.cf;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.conversion.ConversionTools;


public class MapColumnFamily<K, T> extends ColumnFamily<T> implements Map<K, T> {
	protected final Class<K> keyClazz;
	protected final boolean keyIsString;

	public MapColumnFamily(Class<K> keyClazz, Class<T> valueClazz, Field property, String name,
			PersistingElement owner, boolean addOnly, boolean incremental) {
		super(valueClazz, property, name, owner, addOnly, incremental);
		this.keyClazz = keyClazz;
		this.keyIsString = this.keyClazz.equals(String.class);
	}
	
	@Override
	public Serializable getSerializableVersion() {
		TreeMap<K, T> ret = new TreeMap<K, T>();
		for (java.util.Map.Entry<K, T> kv : this.entrySet()) {
			ret.put(kv.getKey(), kv.getValue());
		}
		return ret;
	}
	
	protected String toKey(K key) {
		return this.keyIsString ? (String)key : ConversionTools.convertToString(key, this.keyClazz);
	}
	
	@SuppressWarnings("unchecked")
	protected K fromKey(String key) {
		return this.keyIsString ? (K)key : ConversionTools.convertFromString(this.keyClazz, key);
	}

	@Override
	public void clear() {
		for (String key : this.getKeys()) {
			this.removeKey(key);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean containsKey(Object key) {
		return super.containsKey(this.toKey((K)key));
	}

	@Override
	public boolean containsValue(Object value) {
		return this.collection.containsValue(value);
	}

	@Override
	public Set<K> keySet() {
		Set<K> ret = new HashSet<K>();
		for (String key : this.getKeys()) {
			ret.add(this.fromKey(key));
		}
		return ret;
	}

	@Override
	public Set<java.util.Map.Entry<K, T>> entrySet() {
		Set<java.util.Map.Entry<K, T>> ret = new HashSet<java.util.Map.Entry<K, T>>();
		for (final java.util.Map.Entry<String, T> key : this.collection.entrySet()) {
			ret.add(new Map.Entry<K, T>() {

				@Override
				public K getKey() {
					return MapColumnFamily.this.fromKey(key.getKey());
				}

				@Override
				public T getValue() {
					return key.getValue();
				}

				@Override
				public T setValue(T value) {
					T ret = MapColumnFamily.this.put(getKey(), value);
					if (ret == null ? key.getValue() == null : ret == key.getValue())
						key.setValue(value);
					return ret;
				}
			});
		}
		return ret;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T get(Object key) {
		return this.getElement(this.toKey((K)key));
	}

	@Override
	public T put(K key, T value) {
		String sKey = this.toKey(key);
		T ret = this.get(key);
		this.putElement(sKey, value);
		return ret;
	}

	@Override
	public void putAll(Map<? extends K, ? extends T> m) {
		for (java.util.Map.Entry<? extends K, ? extends T> element : m.entrySet()) {
			this.put(element.getKey(), element.getValue());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public T remove(Object key) {
		T ret = this.get(key);
		this.removeKey(this.toKey((K)key));
		return ret;
	}

	@Override
	public Collection<T> values() {
		return this.collection.values();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void activate(Object from, Object to)
			throws DatabaseNotReachedException {
		if (! this.keyClazz.isInstance(from))
			throw new IllegalArgumentException(from.toString() + " is not compatible with " + this.keyClazz);
		if (! this.keyClazz.isInstance(to))
			throw new IllegalArgumentException(to.toString() + " is not compatible with " + this.keyClazz);
		super.activate(this.toKey((K) from), this.toKey((K) to));
	}

}
