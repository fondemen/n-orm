package com.googlecode.n_orm.cache;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.logging.Logger;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import com.googlecode.n_orm.PersistingElement;

public class Cache {
	private static Logger logger = Logger.getLogger(Cache.class.getName());

	private static int periodBetweenCacheCleanupMS = 1000;
	private static int timeToLiveSeconds = 5;
	private static int maxElementsInCache = 1000;
	
	private static final CacheManager cacheManager = CacheManager.create();
	private static final Map<Thread, Cache> perThreadCaches;
	private static final Timer cacheCleanerTimer;
	private static final TimerTask cacheCleaner;
	
	static {
		perThreadCaches = Collections.synchronizedMap(new LinkedHashMap<Thread, Cache>(16, 0.75f, true) {

			private static final long serialVersionUID = -5467869874569876546l;
	
			@Override
			protected boolean removeEldestEntry(Entry<Thread, Cache> entry) {
				if (!entry.getValue().isValid()) {
					entry.getValue().close();
					return true;
				}
				return false;
			}
			
		});
		cacheCleaner = new TimerTask() {
			
			@Override
			public void run() {
				List<Cache> dead = new LinkedList<Cache>();
				synchronized (perThreadCaches) {
					for (Entry<Thread, Cache> entry : perThreadCaches.entrySet()) {
						if (!entry.getValue().isValid()) {
							dead.add(entry.getValue());
						}
					}
					for (Cache cache : dead) {
						perThreadCaches.remove(cache.thread);
						cache.close();
					}
				}
			}
		};
		cacheCleanerTimer = new Timer("per-thread cache cleaner", true);
		cacheCleanerTimer.schedule(cacheCleaner, new Date(), periodBetweenCacheCleanupMS);
		logger.info("Per-thread caching system started.");
	}
	
	static int getTimeToLiveSeconds() {
		return timeToLiveSeconds;
	}

	static void setTimeToLiveSeconds(int timeToLiveSeconds) {
		Cache.timeToLiveSeconds = timeToLiveSeconds;
	}

	static int getMaxElementsInCache() {
		return maxElementsInCache;
	}

	static void setMaxElementsInCache(int maxElementsInCache) {
		Cache.maxElementsInCache = maxElementsInCache;
	}

	static int getPeriodBetweenCacheCleanupMS() {
		return periodBetweenCacheCleanupMS;
	}

	static void runCacheCleanup() {
		cacheCleaner.run();
	}

	static void waitNextCleanup() throws InterruptedException {
		synchronized(cacheCleaner) {
			cacheCleaner.wait();
		}
	}
	
	static boolean knowsCache(Thread thread) {
		return cacheManager.cacheExists(getThreadId(thread));
	}
	
	static Cache findCache(Thread thread) {
		return perThreadCaches.get(thread);
	}
	
	public static Cache getCache() {
		Cache res = perThreadCaches.get(Thread.currentThread());
		if (res != null && !res.isValid()) {
			perThreadCaches.remove(res);
			res.close();
		}
		if (res == null) {
			res = new Cache();
			perThreadCaches.put(res.thread, res);
		}
		return res;
	}
	
	public static String getThreadId() {
		return getThreadId(Thread.currentThread());
	}
	
	public static String getThreadId(Thread thread) {
		return Long.toHexString(thread.getId());
	}
	
	private final net.sf.ehcache.Cache cache;
	private final Thread thread;
	private boolean closed = false;

	private Cache() {
		this.thread = Thread.currentThread();
		this.cache = new net.sf.ehcache.Cache(
			     new CacheConfiguration(getThreadId(this.thread), maxElementsInCache)
			       .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU)
			       .overflowToDisk(false)
			       .eternal(false)
			       .timeToLiveSeconds(timeToLiveSeconds)
			       .timeToIdleSeconds(timeToLiveSeconds)
			       .diskPersistent(false)
			       .diskExpiryThreadIntervalSeconds(periodBetweenCacheCleanupMS/1000)
			       .statistics(false));
		cacheManager.addCache(this.cache);
		logger.fine("Cache started for " + this.thread + " with id " + this.cache.getName());
	}
	
	boolean isValid() {
		return this.thread.isAlive() && this.thread.getThreadGroup() != null;
	}
	
	protected void checkThread() {
		assert !this.closed;
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
			logger.finest("Found element " + ret + " from cache for thread " + this.thread + " with id " + this.cache.getName());
			return ret;
		}
	}
	
	public PersistingElement getKnownPersistingElement(String identifier, Class<? extends PersistingElement> clazz) {
		return this.getKnownPersistingElement(identifier + clazz.getName());
	}
	
	public int size() {
		return this.cache.getKeysWithExpiryCheck().size();
	}
	
	public void reset() {
		this.cache.removeAll();
		assert this.size() == 0;
		logger.finer("Reseted cache for thread " + this.thread + " with id " + this.cache.getName());
	}
	
	public boolean isClosed() {
		return this.closed;
	}
	
	protected void close() {
		if (this.closed)
			return;
		cacheManager.removeCache(this.cache.getName());
		this.closed = true;
		logger.fine("Cache stopped for thread " + this.thread + " with id " + this.cache.getName());
	}

	@Override
	protected void finalize() throws Throwable {
		this.close();
		super.finalize();
	}
}
