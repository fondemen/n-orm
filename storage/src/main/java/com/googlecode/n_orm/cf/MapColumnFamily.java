package com.googlecode.n_orm.cf;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.consoleannotations.Continuator;
import com.googlecode.n_orm.conversion.ConversionTools;

public class MapColumnFamily<K, T> extends ColumnFamily<T> implements Map<K, T> {
	protected final Class<K> keyClazz;
	protected final boolean keyIsString;
	private Set<Map.Entry<K, T>> entries = null;
	private Set<K> keys = null;

	public MapColumnFamily() {
		keyClazz = null;
		keyIsString = false;
	}

	public MapColumnFamily(Class<K> keyClazz, Class<T> valueClazz,
			Field property, String name, PersistingElement owner) {
		super(valueClazz, property, name, owner);
		this.keyClazz = keyClazz;
		this.keyIsString = this.keyClazz.equals(String.class);
	}

	@Override
	public Serializable getSerializableVersion() {
		HashMap<K, T> ret = new HashMap<K, T>();
		for (Map.Entry<K, T> kv : this.entrySet()) {
			ret.put(kv.getKey(), kv.getValue());
		}
		return ret;
	}

	protected String toKey(K key) {
		return this.keyIsString ? (String) key : ConversionTools
				.convertToString(key, this.keyClazz);
	}

	@SuppressWarnings("unchecked")
	protected K fromKey(String key) {
		return this.keyIsString ? (K) key : ConversionTools.convertFromString(
				this.keyClazz, key);
	}

	@Override
	public boolean equals(Object obj) {
		return this == obj
				|| (obj != null && (obj instanceof Map) && this.entrySet()
						.equals(((Map<?, ?>) obj).entrySet()));
	}

	@Override
	public int hashCode() {
		int h = 0;
		Iterator<Map.Entry<K,T>> i = entrySet().iterator();
		while (i.hasNext())
			h += i.next().hashCode();
		return h;
	}

	@Override
	@Continuator
	public void clear() {
		for (String key : new TreeSet<String>(this.getKeys())) {
			this.removeKey(key);
		}
		assert this.size() == 0;
	}

	@SuppressWarnings("unchecked")
	@Override
	@Continuator
	public boolean containsKey(Object key) {
		return super.containsKey(this.toKey((K) key));
	}

	@Override
	@Continuator
	public boolean containsValue(Object value) {
		return this.collection.containsValue(value);
	}

	@Override
	public Set<K> keySet() {
		if (this.keys == null) {
			this.keys = new Set<K>() {

				@Override
				public boolean add(K e) {
					throw new UnsupportedOperationException();
				}

				@Override
				public boolean addAll(Collection<? extends K> c) {
					throw new UnsupportedOperationException();
				}

				@Override
				public void clear() {
					MapColumnFamily.this.clear();
				}

				@Override
				public boolean contains(Object o) {
					return MapColumnFamily.this.containsKey(o);
				}

				@Override
				public boolean containsAll(Collection<?> c) {
					for (Object k : c) {
						if (!this.contains(k))
							return false;
					}
					return true;
				}

				@Override
				public boolean isEmpty() {
					return this.size() == 0;
				}

				@Override
				public Iterator<K> iterator() {
					final Iterator<String> it = collection.keySet().iterator();
					return new Iterator<K>() {

						@Override
						public boolean hasNext() {
							return it.hasNext();
						}

						@Override
						public K next() {
							return fromKey(it.next());
						}

						@Override
						public void remove() {
							throw new UnsupportedOperationException();
						}
					};
				}

				@Override
				public boolean remove(Object o) {
					if (!this.contains(o))
						return false;
					MapColumnFamily.this.remove(o);
					return true;
				}

				@Override
				public boolean removeAll(Collection<?> c) {
					boolean ret = false;
					for (Object k : c) {
						if (this.remove(k))
							ret = true;
					}
					return ret;
				}

				@Override
				public boolean retainAll(Collection<?> c) {
					boolean ret = false;
					for (K key : new HashSet<K>(this)) {
						if (!c.contains(key)) {
							this.remove(key);
							ret = true;
						}
					}
					return ret;
				}

				@Override
				public int size() {
					return MapColumnFamily.this.size();
				}

				@Override
				public Object[] toArray() {
					return this.toArray(new Object[this.size()]);
				}

				@SuppressWarnings("unchecked")
				@Override
				public <U> U[] toArray(U[] a) {
					if (a.length < this.size())
						a = (U[]) Array.newInstance(a.getClass().getComponentType(), this.size());
					int i = 0;
					for (K element : this) {
						a[i] = (U) element;
						i++;
					}
					if (a.length > i)
						a[i] = null;
					return a;
				}

				@Override
				public boolean equals(Object obj) {
					return obj != null && (obj instanceof Set)
							&& this.hashCode() == obj.hashCode();
				}

				@Override
				public int hashCode() {
					int h = 0;
					Iterator<K> i = iterator();
					while (i.hasNext()) {
						K obj = i.next();
						if (obj != null)
							h += obj.hashCode();
					}
					return h;
				}

			};
		}
		return this.keys;
	}

	@Override
	public Set<Map.Entry<K, T>> entrySet() {
		if (this.entries == null) {
			this.entries = new Set<Map.Entry<K, T>>() {

				@Override
				public boolean add(Map.Entry<K, T> e) {
					return ! e.getValue().equals(put(e.getKey(), e.getValue()));
				}

				@Override
				public boolean addAll(
						Collection<? extends Map.Entry<K, T>> es) {
					boolean ret = false;
					for (Map.Entry<K, T> entry : es) {
						if (this.add(entry))
							ret = true;
					}
					return ret;
				}

				@Override
				public void clear() {
					MapColumnFamily.this.clear();
				}

				@Override
				public boolean contains(Object rhs) {
					if (rhs == null || !(rhs instanceof Map.Entry))
						return false;
					Object key = ((Map.Entry<?,?>) rhs).getKey();
					Object val = ((Map.Entry<?,?>) rhs).getValue();
					T elt = MapColumnFamily.this.get(key);
					return elt != null && val != null && elt.equals(val);
				}

				@Override
				public boolean containsAll(Collection<?> entries) {
					for (Object entry : entries) {
						if (!this.contains(entry))
							return false;
					}
					return true;
				}

				@Override
				public boolean isEmpty() {
					return MapColumnFamily.this.isEmpty();
				}

				@Override
				public Iterator<Map.Entry<K, T>> iterator() {
					final Iterator<Map.Entry<String, T>> it = MapColumnFamily.this.collection
							.entrySet().iterator();
					return new Iterator<Map.Entry<K, T>>() {

						@Override
						public boolean hasNext() {
							return it.hasNext();
						}

						@Override
						public Map.Entry<K, T> next() {
							final Map.Entry<String, T> entry = it.next();
							return new Map.Entry<K, T>() {

								@Override
								public K getKey() {
									return MapColumnFamily.this.fromKey(entry
											.getKey());
								}

								@Override
								public T getValue() {
									return entry.getValue();
								}

								@Override
								public T setValue(T value) {
									if (this.equals(value))
										throw new IllegalArgumentException();
									return MapColumnFamily.this.put(getKey(), value);
								}

								public int hashCode() {
									return (getKey() == null ? 0 : getKey()
											.hashCode())
											^ (getValue() == null ? 0
													: getValue().hashCode());
								}

								@Override
								public boolean equals(Object obj) {
									return obj == this
											|| (obj != null
													&& (obj instanceof Map.Entry<?, ?>)
													&& (getKey() == null ? ((Map.Entry<?, ?>) obj)
															.getKey() == null
															: getKey()
																	.equals(((Map.Entry<?, ?>) obj)
																			.getKey())) && (getValue() == null ? ((Map.Entry<?, ?>) obj)
													.getValue() == null
													: getValue()
															.equals(((Map.Entry<?, ?>) obj)
																	.getValue())));
								}

								@Override
								public String toString() {
									return this.getKey().toString() + '='
											+ this.getValue().toString();
								}
							};
						}

						@Override
						public void remove() {
							throw new UnsupportedOperationException();
						}
					};
				}

				@Override
				public boolean remove(Object elt) {
					if (this.contains(elt)) {
						MapColumnFamily.this.remove(((Map.Entry<?,?>) elt).getKey());
						return true;
					}
					return false;
				}

				@Override
				public boolean removeAll(Collection<?> elts) {
					boolean ret = false;
					for (Object entry : elts) {
						if (this.remove(entry))
							ret = true;
					}
					return ret;
				}

				@Override
				public boolean retainAll(Collection<?> elts) {
					throw new UnsupportedOperationException();
				}

				@Override
				public int size() {
					return MapColumnFamily.this.size();
				}

				@Override
				public Object[] toArray() {
					return this.toArray(new Object[this.size()]);
				}

				@SuppressWarnings("unchecked")
				@Override
				public <U> U[] toArray(U[] ret) {
					if (ret.length < this.size())
						ret = (U[]) Array.newInstance(ret.getClass().getComponentType(), this.size());
					int i = 0;
					Iterator<Map.Entry<K, T>> it = this.iterator();
					while (it.hasNext()) {
						ret[i] = (U) it.next();
						i++;
					}
					if (ret.length > i)
						ret[i] = null;
					return ret;
				}

				@Override
				public boolean equals(Object obj) {
					if (obj == this)
						return true;
					if (obj == null || !(obj instanceof Set))
						return false;
					return this.hashCode() == obj.hashCode();
				}

				@Override
				public int hashCode() {
					int h = 0;
					Iterator<Map.Entry<K, T>> i = iterator();
					while (i.hasNext()) {
						Map.Entry<K, T> obj = i.next();
						if (obj != null)
							h += obj.hashCode();
					}
					return h;
				}
				
				@Override
				public String toString() {
					StringBuffer ret = new StringBuffer();
					ret.append('[');
					boolean fst = true;
					for (Map.Entry<K, T> e : this) {
						if (fst)
							fst = false;
						else
							ret.append(',');
						ret.append(e.toString());
					}
					ret.append(']');
					return ret.toString();
				}

			};
		}
		return this.entries;
	}

	@SuppressWarnings("unchecked")
	@Override
	@Continuator
	public T get(Object key) {
		try {
			return this.getElement(this.toKey((K) key));
		} catch (IllegalArgumentException x) {
			return null;
		}
	}

	@Override
	@Continuator
	public T put(K key, T value) {
		String sKey = this.toKey(key);
		T ret = this.get(key);
		this.putElement(sKey, value);
		return ret;
	}

	@Override
	public void putAll(Map<? extends K, ? extends T> m) {
		for (Map.Entry<? extends K, ? extends T> element : m
				.entrySet()) {
			this.put(element.getKey(), element.getValue());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	@Continuator
	public T remove(Object key) {
		T ret = this.get(key);
		this.removeKey(this.toKey((K) key));
		return ret;
	}

	@Override
	@Continuator
	public Collection<T> values() {
		return this.collection.values();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void activate(Object from, Object to)
			throws DatabaseNotReachedException {
		if (!this.keyClazz.isInstance(from))
			throw new IllegalArgumentException(from.toString()
					+ " is not compatible with " + this.keyClazz);
		if (!this.keyClazz.isInstance(to))
			throw new IllegalArgumentException(to.toString()
					+ " is not compatible with " + this.keyClazz);
		super.activate(this.toKey((K) from), this.toKey((K) to));
	}

	@Override
	protected void updateFromPOJO(Object pojo) {
		Set<String> keys = new TreeSet<String>(this.getKeys());

		@SuppressWarnings("unchecked")
		Map<K, T> pojoM = (Map<K, T>) pojo;
		for (Map.Entry<K, T> element : pojoM.entrySet()) {
			String key = this.toKey(element.getKey());
			if (keys.remove(key)) {
				T known = this.getElement(key);
				if (known == null
						|| this.hasChanged(key, known, element.getValue())) // New
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
		Map<K, T> pojoM = (Map<K, T>) pojo;

		pojoM.clear();
		pojoM.putAll(this);
	}

	@Override
	protected void addToPOJO(Object pojo, String key, T element) {
		@SuppressWarnings("unchecked")
		Map<K, T> pojoM = (Map<K, T>) pojo;

		pojoM.put(this.fromKey(key), element);
	}

}
