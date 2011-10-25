package com.googlecode.n_orm.cache;

import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import javolution.util.FastMap;

import com.googlecode.n_orm.PersistingElement;

public class Cache {
	private static Logger logger = Logger.getLogger(Cache.class.getName());

	private static int periodBetweenCacheCleanupMS = 1000;
	private static int timeToLiveSeconds = 10;
	private static int maxElementsInCache = 10000;
	
	private static final FastMap<Thread, Cache> perThreadCaches;
	private static final Timer cacheCleanerTimer;
	private static final TimerTask cacheCleaner;
	
	static {
		perThreadCaches = new FastMap<Thread, Cache>();
		perThreadCaches.shared();
		cacheCleaner = new TimerTask() {
			
			@Override
			public void run() {
				try {
					Iterator<Entry<Thread, Cache>> ci = perThreadCaches.entrySet().iterator();
					while (ci.hasNext()) {
						Entry<Thread, Cache> entry = ci.next();
						if (entry.getValue().isValid()) {
							entry.getValue().shouldCleanup = true;
						} else {
							entry.getValue().close();
							ci.remove();
						}
					}
				} catch (RuntimeException x) {
					logger.log(Level.WARNING, "Problem while checking cache.", x);
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
		return findCache(thread)!= null;
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
	
	public static String getThreadId(Thread thread) {
		return Long.toHexString(thread.getId());
	}
	
	private static class Element {
		private long lastAccessDate;
		private PersistingElement element;
		public Element(PersistingElement element) {
			super();
			this.element = element;
			this.lastAccessDate = System.currentTimeMillis();
		}
		
		public PersistingElement getElement() {
			this.update();
			return element;
		}
		
		public void setElement(PersistingElement element) {
			this.update();
			this.element = element;
		}
		
		public void update() {
			this.lastAccessDate = System.currentTimeMillis();
		}
		
		public boolean isValid() {
			return (this.lastAccessDate+(timeToLiveSeconds*1000)) > System.currentTimeMillis();
		}
	}
	
	private FastMap<String, Element> cache;
	private final Thread thread;
	private final String threadId;
	private boolean shouldCleanup;

	private Cache() {
		this.thread = Thread.currentThread();
		this.threadId = getThreadId(this.thread);
		this.cache = new FastMap<String, Element>();
		logger.fine("Cache started for " + this.thread + " with id " + this.threadId);
	}
	
	boolean isValid() {
		return this.cache != null && this.thread.isAlive() && this.thread.getThreadGroup() != null;
	}
	
	protected void checkState() {
		assert !this.isClosed();
		if (this.thread != Thread.currentThread())
			throw new IllegalStateException(Thread.currentThread().toString() + " with id " + Thread.currentThread().getId() + " is not allowed to acccess cache for " + (this.thread.isAlive() ? "alive" : "dead") + " " + this.thread + " with id " + this.thread.getId());
		this.cleanIfNecessary();
	}
	
	private void cleanIfNecessary() {
		if (this.shouldCleanup)
			this.cleanInvalidElements();
	}
	
	/**
	 * @return the element with the eldest last usage (null if cache is empty)
	 */
	private Element cleanInvalidElements() {
		this.shouldCleanup = true;
		Element eldest = null;
		Iterator<Entry<String, Element>> it = cache.entrySet().iterator();
		while (it.hasNext()) {
			Element elt = it.next().getValue();
			if (!elt.isValid()) {
				it.remove();
			} else if (eldest == null) {
				eldest = elt;
			} else if (eldest.lastAccessDate > elt.lastAccessDate) {
				eldest = elt;
			}
		}
		logger.fine("Cleaned cache for thread " + this.thread + " with id " + threadId);
		return eldest;
	}
	
	public void register(PersistingElement element) {
		this.checkState();
		
		if (element == null)
			return;
		String id = element.getFullIdentifier();
		Element cached = this.cache.get(id);
		if (cached == null) {
			cached = new Element(element);
			this.cache.put(id, cached);
		} else {
			cached.setElement(element);
		}
		while (this.cache.size() > maxElementsInCache) //Should happen only once
			this.cache.remove(this.cleanInvalidElements().getElement().getFullIdentifier());
		logger.finer("Registered element " + element + " for thread " + this.thread + " with id " + threadId);
	}
	
	public void unregister(PersistingElement element) {
		this.checkState();
		
		if (this.cache.remove(element.getFullIdentifier()) != null) {
			logger.finer("Unregistered element with " + element + " for thread " + this.thread + " with id " + threadId);
		}
	}
	
	public PersistingElement getKnownPersistingElement(String fullIdentifier) {
		this.checkState();
		Element res = (Element) this.cache.get(fullIdentifier);
		if (res == null)
			return null;
		else {
			PersistingElement ret = res.getElement();
			logger.finest("Found element " + ret + " from cache for thread " + this.thread + " with id " + this.threadId);
			return ret;
		}
	}
	
	public PersistingElement getKnownPersistingElement(String identifier, Class<? extends PersistingElement> clazz) {
		return this.getKnownPersistingElement(identifier + clazz.getName());
	}
	
	public int size() {
		this.cleanInvalidElements();
		return this.cache.size();
	}
	
	public void reset() {
		this.cache.clear();
		logger.finer("Reseted cache for thread " + this.thread + " with id " + threadId);
	}
	
	public boolean isClosed() {
		return this.cache == null;
	}
	
	protected void close() {
		if (this.cache == null)
			return;
		this.reset();
		this.cache = null;
		logger.fine("Cache stopped for thread " + this.thread + " with id " + this.threadId);
	}

	@Override
	protected void finalize() throws Throwable {
		this.close();
		super.finalize();
	}
}
