package com.googlecode.n_orm.utils;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.googlecode.n_orm.Transient;

/**
 * A stack of maps overriding each other.
 * Adding a new map on the to of the stack is possible by using the thread-safe {@link #override(Map)} method.
 * 
 * @param <K> Key for the held maps
 * @param <V> Value for the held maps
 */
public class UnmodifiableOverridingMap<K, V> extends AbstractMap<K, V> implements Map<K, V> {
	
	private static final Object O = new Object();

	private class MapStack {
		@Transient public final Map<K, V> map;
		public final MapStack previous;
		private MapStack(Map<K, V> map, MapStack previous) {
			super();
			this.map = Collections.unmodifiableMap(map);
			this.previous = previous;
		}
		
		public boolean containsValue(Object value) {
			return this.map.containsValue(value) || (this.previous != null && this.previous.containsValue(value));
		}
		
		public V getValue(Object key) {
			if (this.map.containsKey(key))
				return this.map.get(key);
			else if (this.previous != null)
				return this.previous.getValue(key);
			else
				return null;
		}
	}
	
	private final AtomicReference<MapStack> maps = new AtomicReference<MapStack>();
	
	@Transient private Map<K, Object> keys = new ConcurrentHashMap<K, Object>();

	/**
	 * Thread-safe mean to add new values by overriding existing ones.
	 */
	public void override(Map<K, V> overridingMap) {
		if (overridingMap == null)
			return;
		MapStack newMs;
		do {
			newMs = new MapStack(overridingMap, this.maps.get());
		} while (!this.maps.compareAndSet(newMs.previous, newMs));
		for (K key : overridingMap.keySet()) {
			this.keys.put(key, O);
		}
	}

	@Override
	public int size() {
		return this.keys.size();
	}

	@Override
	public boolean isEmpty() {
		return this.keys.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return this.keys.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		MapStack m = this.maps.get();
		return m != null && m.containsValue(value);
	}

	@Override
	public V get(Object key) {
		MapStack m = this.maps.get();
		return m == null ? null : m.getValue(key);
	}

	@Override
	public V put(K key, V value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public V remove(Object key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<K> keySet() {
		return Collections.unmodifiableSet(this.keys.keySet());
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return new AbstractSet<Map.Entry<K,V>>() {

			@Override
			public int size() {
				return UnmodifiableOverridingMap.this.size();
			}

			@Override
			public boolean isEmpty() {
				return UnmodifiableOverridingMap.this.isEmpty();
			}

			@Override
			public boolean contains(Object o) {
				if (!(o instanceof java.util.Map.Entry))
					return false;
				Entry<?, ?> e = (Entry<?, ?>)o;
				if (!UnmodifiableOverridingMap.this.containsKey(e.getKey()))
					return false;
				V val = UnmodifiableOverridingMap.this.get(e.getKey());
				return val == null ? e.getValue() == null : val.equals(e.getValue());
			}

			@Override
			public Iterator<java.util.Map.Entry<K, V>> iterator() {
				return new Iterator<Map.Entry<K,V>>() {
					Iterator<K> it = UnmodifiableOverridingMap.this.keys.keySet().iterator();

					@Override
					public boolean hasNext() {
						return it.hasNext();
					}

					@Override
					public java.util.Map.Entry<K, V> next() {
						final K key = it.next();
						return new Entry<K, V>() {
							
							@Override
							public K getKey() {
								return key;
							}

							@Override
							public V getValue() {
								return UnmodifiableOverridingMap.this.get(key);
							}

							@Override
							public V setValue(V value) {
								throw new UnsupportedOperationException();
							}

							@Override
					        public final int hashCode() {
								V value = this.getValue();
					            return (key == null   ? 0 : key.hashCode()) ^
					                   (value == null ? 0 : value.hashCode());
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
			public boolean add(java.util.Map.Entry<K, V> e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean remove(Object o) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean addAll(
					Collection<? extends java.util.Map.Entry<K, V>> c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean retainAll(Collection<?> c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean removeAll(Collection<?> c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void clear() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
