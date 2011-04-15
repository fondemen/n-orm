package com.googlecode.n_orm.cf;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.conversion.ConversionTools;


public class MapColumnFamily<K, T> extends ColumnFamily<T> implements Map<K, T> {
	protected final Class<K> keyClazz;
	protected final boolean keyIsString;
	
	public MapColumnFamily() {
		keyClazz = null;
		keyIsString = false;
	}

	public MapColumnFamily(Class<K> keyClazz, Class<T> valueClazz, Field property, String name,
			PersistingElement owner) {
		super(valueClazz, property, name, owner);
		this.keyClazz = keyClazz;
		this.keyIsString = this.keyClazz.equals(String.class);
	}
	
	@Override
	public Serializable getSerializableVersion() {
		HashMap<K, T> ret = new HashMap<K, T>();
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
		for (String key : new TreeSet<String>(this.getKeys())) {
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

	@Override
	protected void updateFromPOJO(Object pojo) {
		Set<String> keys = new TreeSet<String>(this.getKeys());
		
		@SuppressWarnings("unchecked")
		Map<K, T> pojoM = (Map<K, T>)pojo;
		for (java.util.Map.Entry<K, T> element : new ArrayList<java.util.Map.Entry<K, T>>(pojoM.entrySet())) {
			String key = this.toKey(element.getKey());
			if (keys.remove(key)) {
				T known = this.getElement(key);
				if (known == null || this.hasChanged(key, known, element.getValue())) //New
					this.putElement(key, element.getValue());
			} else {
				this.putElement(key, element.getValue());
			}
		}
		
		for (String key : keys) {
			this.removeKey(key);
		}
	}

	@Override
	protected void storeToPOJO(Object pojo) {
		@SuppressWarnings("unchecked")
		Map<K, T> pojoM = (Map<K, T>)pojo;
		
		pojoM.clear();
		pojoM.putAll(this);
	}

	@Override
	protected void addToPOJO(Object pojo, String key, T element) {
		@SuppressWarnings("unchecked")
		Map<K, T> pojoM = (Map<K, T>)pojo;
		
		pojoM.put(this.fromKey(key), element);
	}

}
