package com.googlecode.n_orm.cache;

import java.util.logging.Logger;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.event.CacheEventListener;
import net.sf.ehcache.store.LruPolicy;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import com.googlecode.n_orm.PersistingElement;

public class Cache {
	private static Logger logger = Logger.getLogger(Cache.class.getName());
	
	private static CacheManager cacheManager = CacheManager.create();
	private static net.sf.ehcache.Cache perThreadCaches;
	
	static {
		perThreadCaches = new net.sf.ehcache.Cache(
			     new CacheConfiguration("thread-caches", 10000)
			       .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU)
			       .overflowToDisk(false)
			       .eternal(false)
			       .timeToLiveSeconds(120)
			       .timeToIdleSeconds(120)
			       .diskPersistent(false)
			       .diskExpiryThreadIntervalSeconds(0)
			       .statistics(false));
		cacheManager.addCache(perThreadCaches);
		perThreadCaches.setMemoryStoreEvictionPolicy(new LruPolicy() {
			public static final String NAME = "Alive";
			
			@Override
			public String getName() {
				return NAME;
			}
			
			@Override
			public boolean compare(Element element1, Element element2) {
				return ((Cache)element1.getValue()).isValid() ?
						false
					:	(((Cache)element2.getValue()).isValid() ?
								true 
							:	super.compare(element1, element2));
					
			}
		});
		perThreadCaches.getCacheEventNotificationService().registerListener(new CacheEventListener() {
			
			@Override
			public void notifyRemoveAll(Ehcache cache) {
			}
			
			@Override
			public void notifyElementUpdated(Ehcache cache, Element element)
					throws CacheException {
			}
			
			@Override
			public void notifyElementRemoved(Ehcache cache, Element element)
					throws CacheException {
				((Cache)element.getValue()).close();
			}
			
			@Override
			public void notifyElementPut(Ehcache cache, Element element)
					throws CacheException {
			}
			
			@Override
			public void notifyElementExpired(Ehcache cache, Element element) {
			}
			
			@Override
			public void notifyElementEvicted(Ehcache cache, Element element) {
			}
			
			@Override
			public void dispose() {
			}
			
			@Override
			public Object clone() throws CloneNotSupportedException {
				throw new CloneNotSupportedException();
			}
		});
		logger.info("Per-thread caching system started.");
	}
	
	public static Cache getCache() {
		Element res = perThreadCaches.get(Thread.currentThread());
		if (res != null && !((Cache)res.getObjectValue()).isValid()) {
			perThreadCaches.remove(res);
			((Cache)res.getObjectValue()).close();
			res = null;
		}
		if (res == null) {
			res = new Element(Thread.currentThread(), new Cache());
			perThreadCaches.put(res);
		}
		return (Cache) res.getObjectValue();
	}
	
	public static String getThreadId() {
		return getThreadId(Thread.currentThread());
	}
	
	public static String getThreadId(Thread thread) {
		return Long.toHexString(thread.getId());
	}
	
	private final net.sf.ehcache.Cache cache;
	private final Thread thread;

	private Cache() {
		this.thread = Thread.currentThread();
		this.cache = new net.sf.ehcache.Cache(
			     new CacheConfiguration(getThreadId(this.thread), 10000)
			       .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU)
			       .overflowToDisk(false)
			       .eternal(false)
			       .timeToLiveSeconds(60)
			       .timeToIdleSeconds(30)
			       .diskPersistent(false)
			       .diskExpiryThreadIntervalSeconds(0)
			       .statistics(false));
		cacheManager.addCache(this.cache);
		logger.info("Cache started for " + this.thread + " with id " + this.cache.getName());
	}
	
	boolean isValid() {
		return this.thread.isAlive() && this.thread.getThreadGroup() != null;
	}
	
	protected void checkThread() {
		if (this.thread != Thread.currentThread())
			throw new IllegalStateException(Thread.currentThread().toString() + " with id " + Thread.currentThread().getId() + " is not allowed to acccess cache for " + (this.thread.isAlive() ? "alive" : "dead") + " " + this.thread + " with id " + this.thread.getId());
	}
	
	public void register(PersistingElement element) {
		this.checkThread();
		if (element == null)
			return;
		if (!element.isKnownAsNotExistingInStore()) {
			this.cache.put(new Element(element.getFullIdentifier(), element));
			logger.finer("Registered element " + element + " for thread " + this.thread + " with id " + this.cache.getName());
		}
	}
	
	public void unregister(PersistingElement element) {
		this.checkThread();
		if (this.cache.remove((Object)element.getFullIdentifier())) {
			logger.finer("Unregistered element with " + element + " for thread " + this.thread + " with id " + this.cache.getName());
		}
	}
	
	public PersistingElement getKnownPersistingElement(String fullIdentifier) {
		this.checkThread();
		Element res = this.cache.get(fullIdentifier);
		if (res == null)
			return null;
		else {
			PersistingElement ret = (PersistingElement) res.getValue();
			logger.finer("Found element " + ret + " from cache for thread " + this.thread + " with id " + this.cache.getName());
			return ret;
		}
	}
	
	public PersistingElement getKnownPersistingElement(String identifier, Class<? extends PersistingElement> clazz) {
		return this.getKnownPersistingElement(identifier + clazz.getName());
	}
	
	public void reset() {
		this.cache.removeAll();
	}
	
	protected void close() {
		cacheManager.removeCache(this.cache.getName());
		logger.info("Logger stopped for thread " + this.thread + " with id " + this.cache.getName());
	}

	@Override
	protected void finalize() throws Throwable {
		this.close();
		super.finalize();
	}
}
