package com.googlecode.n_orm.cache;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.googlecode.n_orm.PersistingElement;

/**
 * A cache for temporarily storing {@link PersistingElement}s.<br>
 * There is a cache per thread, so that cached elements are thread safe unless explicitly shared across threads.
 * Only thread owning a cache is allowed to use this cache.<br>
 * To get a cache, use {@link #getCache()} within the using thread.<br>
 * Caches are limited in size (see {@link #getMaxElementsInCache()}).
 * Cached elements are removed as soon as they have not been accessed since a certain amount of time (see {@link #getTimeToLiveSeconds()}).<br>
 * Caches for dead thread are recycled, but in case they have not been used during a certain amount of time (see {@link #getTimeToLiveSeconds()}), they are dropped.
 * @author fondemen
 *
 */
public class Cache {
	
	private static class CleanerThread extends Thread {

		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(periodBetweenCacheCleanupMS);
					this.step();
				} catch (Throwable e) {
					logger.log(Level.WARNING, "Problem while waiting next cache cleanup.", e);
				}
			}
		}

		public void step() {
			
			try {
				long now = System.currentTimeMillis();
				LIFO<Cache> available = new LIFO<Cache>();
				
				try {
					
					Thread[] threads;
					synchronized(perThreadCaches) {
						Set<Thread> threadsSet = perThreadCaches.keySet();
						threads = threadsSet.toArray(new Thread[threadsSet.size()]);
					}
					for(Thread t : threads) {
						Cache cache;
						if (!isValid(t)) {
							synchronized(perThreadCaches) {
								cache = perThreadCaches.remove(t);
							}
							if (cache != null) {
								available.push(cache);
								cache.stopped = now;
							}
						} else {
							cache = perThreadCaches.get(t);
							if (cache == null) {
								//Nothing to do ; cache is gone
							} else if (cache.cache != null && cache.stopped == -1) {
								cache.shouldCleanup = true;
							} else {
								assert false;
								synchronized(perThreadCaches) {
									perThreadCaches.remove(t);
								}
							}
						}
					}
				} catch (RuntimeException x) {
					logger.log(Level.WARNING, "Problem while checking cache.", x);
				}
				
				synchronized(availableCaches) {
					try {
						availableCaches.pushAll(available);
					} catch (RuntimeException x) {
						logger.log(Level.WARNING, "Problem while checking cache.", x);
					}
					available.clear();
				}
				
				try {
					Iterator<Cache> ai = availableCaches.iterator();
availableCachesCheck:while (ai.hasNext()) {
						Cache cache = ai.next();
						if (cache == null) {
							logger.warning("Invalid state: got an empty object while iterating over available caches.");
							if (ai.hasNext()) {
								cache = ai.next();
								if (cache == null) {
									String cachesStr;
									try {
										cachesStr = availableCaches.toString();
										logger.severe("Invalid state: cannot continue iterating over available caches " + cachesStr);
									} catch (Throwable t) {
										logger.log(Level.SEVERE, "It seems that available caches are lost. No mean to recover, resetting.", t);
										availableCaches.clear();
									}
									break availableCachesCheck;
								}
							} else
								break availableCachesCheck;
						}
						synchronized(cache) {
							if ((cache.stopped+(timeToLiveSeconds*1000)) < now) {
								ai.remove();
								cache.close();
							} else if (cache.isClosed()) {
								ai.remove();
							} else if (isValid(cache.thread)) {
								ai.remove();
								assert perThreadCaches.get(cache.thread) == cache;
							}
						}
					}
				} catch (RuntimeException x) {
					logger.log(Level.WARNING, "Problem while checking cache.", x);
				}
			} finally {
			
				synchronized(this) {
					this.notifyAll();
				}
			}
		}
	}

	private static Logger logger = Logger.getLogger(Cache.class.getName());

	private static int periodBetweenCacheCleanupMS = 1000;
	private static int timeToLiveSeconds = 10;
	private static int maxElementsInCache = 10000;

	
	private static final HashMap<Thread, Cache> perThreadCaches;
	private static final LIFO<Cache> availableCaches;
	private static final Thread cacheCleaner;
	
	static {
		perThreadCaches = new HashMap<Thread, Cache>();
		availableCaches = new LIFO<Cache>();
		cacheCleaner = new CleanerThread();
		cacheCleaner.setDaemon(true);
		cacheCleaner.start();
		logger.info("Per-thread caching system started.");
	}
	
	private static boolean isValid(Thread thread) {
		return thread != null && thread.isAlive() && thread.getThreadGroup() != null;
	}
	
	/**
	 * Time during which a element is kept in the cache (10s by default).
	 * If an element is accessed (e.g. by storing or activating it), its time to live is reseted.
	 * A cache is also kept during this time once its corresponding thread is over, so that it can be reused by another thread.
	 * @return time to live in seconds
	 */
	public static int getTimeToLiveSeconds() {
		return timeToLiveSeconds;
	}

	/**
	 * Sets time during which a element is kept in the cache.
	 * If an element is accessed (e.g. by storing or activating it), its time to live is reseted.
	 * A cache is also kept during this time once its corresponding thread is over, so that it can be reused by another thread.
	 * @param timeToLiveSeconds time to live in seconds
	 */
	public static void setTimeToLiveSeconds(int timeToLiveSeconds) {
		Cache.timeToLiveSeconds = timeToLiveSeconds;
	}

	/**
	 * The maximum number of elements that can be kept in a (per-thread) cache (10 000 by default).
	 */
	public static int getMaxElementsInCache() {
		return maxElementsInCache;
	}

	/**
	 * Sets the maximum number of elements that can be kept in a (per-thread) cache.
	 */
	public static void setMaxElementsInCache(int maxElementsInCache) {
		Cache.maxElementsInCache = maxElementsInCache;
	}

	/**
	 * The time between two cache cleanups (1s by default).
	 * Caches with alive thread are asked to cleanup at next access: all elements that have not been accessed since their time to live are discarded.
	 * Caches with dead threads are checked for time to live and discarded if this period is over, otherwise their elements are immediately checked for time to live.
	 * @return period between two cache cleanups in milliseconds
	 */
	public static int getPeriodBetweenCacheCleanupMS() {
		return periodBetweenCacheCleanupMS;
	}

	/**
	 * The time between two cache cleanups.
	 * Caches with alive thread are asked to cleanup at next access: all elements that have not been accessed since their time to live are discarded.
	 * Caches with dead threads are checked for time to live and discarded if this period is over, otherwise their elements are immediately checked for time to live.
	 * @param periodBetweenCacheCleanupMS period between two cache cleanups in milliseconds
	 */
	public static void setPeriodBetweenCacheCleanupMS(
			int periodBetweenCacheCleanupMS) {
		Cache.periodBetweenCacheCleanupMS = periodBetweenCacheCleanupMS;
	}

	static void waitNextCleanup() throws InterruptedException {
		synchronized(cacheCleaner) {
			cacheCleaner.wait();
		}
	}
	
	static boolean knowsCache(Thread thread) {
		return findCache(thread)!= null;
	}
	
	static void cleanRecyclableCaches() {
		synchronized(availableCaches) {
			availableCaches.clear();
		}
	}
	
	public static Cache findCache(Thread thread) {
		synchronized(perThreadCaches) {
			return perThreadCaches.get(thread);
		}
	}
	
	/**
	 * Gives a cache to the current thread.
	 * In case this cache does not already exists, creates it.
	 */
	public static Cache getCache() {
		Cache res = findCache(Thread.currentThread());
		
		if (res == null) {
			res = (Cache) availableCaches.pop(); //Most recent in the queue ; if closed, all the others in availableCaches should be closed...
			if (res != null) {
				synchronized(res) {
					if (res.isClosed())
						res = null;
					else {
						res.init();
						res.cleanInvalidElements();
						logger.finer("Reusing existing cache for thread " + res.thread);
					}
				}
			}
		}
		
		if (res == null) {
			res = new Cache();
		}
		
		assert isValid(res.thread);
		assert !res.isClosed();
		assert !availableCaches.contains(res);
		assert perThreadCaches.get(Thread.currentThread()) == res;
		
		return res;
	}
	
	/**
	 * A string identifier for the thread.
	 */
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
	
	private Map<String, Element> cache;
	private Thread thread;
	private String threadId;
	private volatile boolean shouldCleanup;
	private volatile long stopped = -1;

	private Cache() {
		this.init();
	}
	
	private synchronized void init() {
		this.thread = Thread.currentThread();
		this.threadId = getThreadId(this.thread);
		
		this.cache = new HashMap<String, Cache.Element>();
		this.stopped = -1;
		
		synchronized(perThreadCaches) {
			perThreadCaches.put(this.thread, this);
		}
		logger.fine("Cache started for " + this.thread + " with id " + this.threadId);
	}
	
	protected void checkState() {
		assert !this.isClosed();
		Thread cur = Thread.currentThread();
		if (this.thread != cur)
			throw new IllegalStateException(cur.toString() + " with id " + cur.getId() + " is not allowed to acccess cache for " + (this.thread.isAlive() ? "alive" : "dead") + " " + this.thread + " with id " + this.thread.getId());
		this.cleanIfNecessary();
	}
	
	private void cleanIfNecessary() {
		if (this.shouldCleanup)
			this.cleanInvalidElements();
	}
	
	/**
	 * @return the element with the eldest last usage (null if cache is empty)
	 */
	private synchronized Element cleanInvalidElements() {
		this.shouldCleanup = false;
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
	
	/**
	 * Registers an element in the cache.<br>
	 * Only thread for this cache has access to this method.<br>
	 * In case this cache is marked for cleanup, elements are checked for their time to live (see {@link #setTimeToLiveSeconds(int)}).<br>
	 * In case the maximum number of cacheable elements is reached, element with the eldest access id removed from the cache (see {@link #setMaxElementsInCache(int)})
	 * @param element the element to be cached (during {@link #getTimeToLiveSeconds()})
	 * @throws IllegalStateException in case this thread is not the thread for this cache
	 */
	public void register(PersistingElement element) throws IllegalStateException {
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
	
	/**
	 * Removes an element in the cache.<br>
	 * Only thread for this cache has access to this method.<br>
	 * In case this cache is marked for cleanup, elements are checked for their time to live (see {@link #setTimeToLiveSeconds(int)}).<br>
	 * @param element the element to be uncached
	 * @throws IllegalStateException in case this thread is not the thread for this cache
	 */
	public void unregister(PersistingElement element) throws IllegalStateException {
		this.checkState();
		
		if (this.cache.remove(element.getFullIdentifier()) != null) {
			logger.finer("Unregistered element with " + element + " for thread " + this.thread + " with id " + threadId);
		}
	}
	
	/**
	 * Finds an element in the cache according to its full identifier.<br>
	 * Only thread for this cache has access to this method.<br>
	 * In case this cache is marked for cleanup, elements are checked for their time to live (see {@link #setTimeToLiveSeconds(int)}).<br>
	 * @param fullIdentifier the full identifier of the element to be cached; null if not found
	 * @throws IllegalStateException in case this thread is not the thread for this cache
	 * @see PersistingElement#getFullIdentifier()
	 */
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

	/**
	 * Finds an element in the cache according to its identifier.<br>
	 * Only thread for this cache has access to this method.<br>
	 * In case this cache is marked for cleanup, elements are checked for their time to live (see {@link #setTimeToLiveSeconds(int)}).<br>
	 * @param identifier the full identifier of the element to be cached; null if not found
	 * @param clazz the class that the element instantiates (see {@link Object#getClass())}; null if not found
	 * @throws IllegalStateException in case this thread is not the thread for this cache
	 * @see PersistingElement#getIdentifier()
	 */
	public PersistingElement getKnownPersistingElement(String identifier, Class<? extends PersistingElement> clazz) {
		return this.getKnownPersistingElement(identifier + clazz.getName());
	}
	
	/**
	 * The size of the cache.
	 * This method checks elements for their time to live regardless the cache is marked for that or not.
	 * @return
	 */
	public int size() {
		this.cleanInvalidElements();
		return this.cache.size();
	}
	
	/**
	 * Clears the cache.
	 */
	public void reset() {
		this.cache.clear();
		logger.finer("Reseted cache for thread " + this.thread + " with id " + threadId);
	}
	
	/**
	 * Checks if this cache can be used or not.
	 * A cache cannot be used if its thread is dead and if its time to live is over.
	 * A closed cache must not be used in any case (necessary resources were released)
	 * @return whether this cache has bee closed
	 * @see #close()
	 */
	public synchronized boolean isClosed() {
		return this.cache == null || this.thread == null;
	}
	
	/**
	 * Closes the cache.
	 * This method is automatically invoked on caches for dead threads and whose time to live is over.
	 * A closed cache cannot be used anymore.
	 * @see #isClosed()
	 */
	protected synchronized void close() {
		if (this.isClosed())
			return;
		
		this.reset();
		this.cache = null;
		this.thread = null;
		this.stopped = System.currentTimeMillis();
		logger.fine("Cache stopped for thread " + this.thread + " with id " + this.threadId);
	}

	@Override
	protected void finalize() throws Throwable {
		this.close();
		super.finalize();
	}
}
