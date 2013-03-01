package com.googlecode.n_orm.hbase;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.recipes.lock.SharedExclusiveLock;

import com.googlecode.n_orm.DatabaseNotReachedException;

/**
 * A zookeeper-based read write lock for protecting table schemas operations.
 * Read lock can be promoted to write lock ;
 * in this latter case write unlock brings back read lock.
 * Uses the "/n-orm/schemalock/<table name>" directory
 * at the zookeeper level.<br>
 * For a given thread, locks can be acquired a reentrant way.
 * However, they must be released in the reversed order the've been acquired.
 * As an example, a thread can perform the following scenario:<ol>
 * <li>sharedLockTable (actually gets a shared lock here)
 * <li>sharedLockTable
 * <li>exclusiveLockTable (actually releases the shared lock and then gets an exclusive lock here)
 * <li>sharedLockTable
 * <li>exclusiveLockTable
 * 
 * <li>exclusiveUnlockTable (cannot call sharedUnlockTable here)
 * <li>sharedUnlockTable
 * <li>exclusiveUnlockTable (actually releases the exclusive lock and then gets a shared lock here)
 * <li>sharedUnlockTable
 * <li>sharedUnlockTable (actually releases the shared lock here)
 * </ol><br>
 * It is enforced that if a thread holds an exclusive lock,
 * an exclusive lock is acquired at the zookeeper level ;
 * otherwise, if there is a thread that holds a shared lock,
 * a shared lock is acquired at the zookeeper level ;
 * otherwise, no lock is held from zookeeper.
 */
public class TableLocker {
	public static final long lockTimeout = 10000;
	
	private static enum LockKind {
		SHARED, EXCLUSIVE
	}
	
	/**
	 * The store to which belongs this lock
	 */
	private final Store store;
	/**
	 * The table for which this lock is made for
	 */
	private final String table;
	
	/**
	 * The actual zookeeper lock.
	 */
	private final SharedExclusiveLock lock;
	
	/**
	 * The Zookeeper SharedExclusiveLock recipe does not handle
	 * intra-JVM interlocking ; using this ReentrantReadWriteLock to overcome this drawback. 
	 */
	private final ReentrantReadWriteLock intraLock = new ReentrantReadWriteLock();
	
	/**
	 * The list of thread holding a shared lock
	 */
	private final Map<Thread, LinkedList<LockKind>> acquiredLocks = Collections.synchronizedMap(new HashMap<Thread, LinkedList<LockKind>>());
	
	/**
	 * The number of acquired shared locks
	 */
	private final AtomicInteger sharedLocks = new AtomicInteger(0);
	
	/**
	 * @param store the store to which this lock is to belong
	 * @param table the table to be shared/exclusive locked
	 */
	public TableLocker(Store store, String table) {
		super();
		this.store = store;
		this.table = store.mangleTableName(table);
		
		this.lock = new SharedExclusiveLock(store.getZooKeeper(), "/n-orm/schemalock/" + table);
	}
	
	/**
	 * Checks that we have the proper connection to zookeeper (that one of the store).
	 */
	protected void checkZooKeeper() {
		ZooKeeper zk = this.store.getZooKeeper();
		
		if (lock.getZookeeper() != zk) {
			lock.setZookeeper(zk);
		}
	}
	
	/**
	 * Checks that zookeeper directories are here.
	 * Triggers (at least) a zookeeper query.
	 */
	protected void checkIsZKLockable() {
		this.checkZooKeeper();
		ZooKeeper zk = lock.getZookeeper();

		try {
			assert lock.getPath() != null && lock.getPath().startsWith("/n-orm/schemalock");
			
			while (zk.exists(lock.getPath(), false) == null) {
				while (zk.exists("/n-orm/schemalock", false) == null)  {
					while (zk.exists("/n-orm", false) == null)  {
						try {
							zk.create("/n-orm", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						} catch (NodeExistsException x){}
					}
					try {
						zk.create("/n-orm/schemalock", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					} catch (NodeExistsException x){}
				}
				try {
					String node = zk.create(lock.getPath(), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					Store.logger.info("Created lock node " + node);
				} catch (NodeExistsException x){}
			}
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}
	}
	
	/**
	 * Locks the table: first gets the local shared lock, then acquires
	 * a zookeeper shared lock on the table.
	 */
	public void sharedLockTable() throws DatabaseNotReachedException {
		this.sharedLocks.getAndIncrement();
		
		LinkedList<LockKind> locks = this.acquiredLocks.get(Thread.currentThread());
		if (locks != null) {
			// This thread already holds a lock
			// referencing this request as the last for this thread
			// and returning immediately
			assert !locks.isEmpty();
			locks.addFirst(LockKind.SHARED);
			return;
		}
		
		// Getting local lock
		this.intraLock.readLock().lock();
		// Handling sharedLockThreads and zookeeper lock in mutual exclusion
		if (!this.isShareLocked()) {
			synchronized (this) {
				// One should acquire zookeeper shared lock if and only if it's the first to ask for the lock
				if (!this.isShareLocked()) {
					assert ! this.isExclusiveLocked() : "Can't acquire shared lock on table " + this.table + " while it's already exclusively locked";
					try {
						// Checking zookeeper connection and directories
						this.checkIsZKLockable();
						// Acquiring zookeeper shared lock
						lock.getSharedLock(lockTimeout);
					} catch (Exception e) {
						throw new DatabaseNotReachedException(e);
					}
				}
			}
		}
		
		locks = new LinkedList<LockKind>();
		locks.addFirst(LockKind.SHARED);
		this.acquiredLocks.put(Thread.currentThread(), locks);
		
		assert ! this.isExclusiveLocked() : "Acquired a shared lock on table " + this.table + " while an exclusive lock is already set !";
		assert this.isShareLocked() : "Acquired a shared lock on table " + this.table + " but not referenced by zookeeper !";
		assert this.acquiredLocks.containsKey(Thread.currentThread()) : "Acquired a shared lock on table " + this.table + " but thread is not referenced as having one";
		assert LockKind.SHARED.equals(this.acquiredLocks.get(Thread.currentThread()).peek());
	}
	
	public void sharedUnlockTable() throws DatabaseNotReachedException {
		LinkedList<LockKind> locks = this.acquiredLocks.get(Thread.currentThread());
		// Checking that last acquired lock is a shared lock
		LockKind last = locks.peek();
		if (!LockKind.SHARED.equals(last)) {
			throw new IllegalMonitorStateException("Last lock for table " + this.table + " for this thread is not a shared lock");
		}

		int sl = this.sharedLocks.decrementAndGet();
		
		assert sl >= 0;
		assert isShareLocked();
		assert ! isExclusiveLocked();
		
		// Removing the last shared lock
		locks.poll();
		// If we hold another lock, returning
		if (locks.isEmpty())
			this.acquiredLocks.remove(Thread.currentThread());
		else
			return;
		
		// We are the last process to hold a shared lock
		if (sl == 0) {
			synchronized (this) {
				assert isShareLocked();
				try {
					this.checkZooKeeper();
					lock.releaseSharedLock();
				} catch (Exception e) {
					try {
						this.checkZooKeeper();
						lock.releaseSharedLock();
					} catch (Exception f) {
						Store.errorLogger.log(Level.SEVERE, "Error unlocking table " + table, e);
					}
				}
			}
		}
		
		this.intraLock.readLock().unlock();
		
		assert ! this.isExclusiveLocked();
	}

	public boolean isShareLocked() {
		return lock.getCurrentSharedLock() != null;
	}
	
	public void exclusiveLockTable() throws DatabaseNotReachedException {
		boolean wasReadLocked;
		LinkedList<LockKind> locks = this.acquiredLocks.get(Thread.currentThread());
		if (locks != null) {
			// This thread already holds a lock
			// referencing this request as the last for this thread
			// and returning immediately if we already own the exclusive lock
			assert !locks.isEmpty();
			if (this.intraLock.isWriteLockedByCurrentThread()) {
				// We already hold an exclusive lock
				// It should be already referenced
				assert locks.contains(LockKind.EXCLUSIVE);
				locks.addFirst(LockKind.EXCLUSIVE);
				return;
			}
			wasReadLocked = locks.contains(LockKind.SHARED);
		} else {
			wasReadLocked = false;
		}
		
		assert !this.intraLock.isWriteLockedByCurrentThread();
		assert locks == null || ! locks.contains(LockKind.EXCLUSIVE);
			
		if (wasReadLocked)
			this.intraLock.readLock().unlock();
		this.intraLock.writeLock().lock();

		//synchronized (this) { // Useless as we already own the internal write lock
			if (isShareLocked()) {
				assert this.sharedLocks.get() > 0;
				try {
					this.checkZooKeeper();
					lock.releaseSharedLock();
				} catch (Exception e) {
					throw new DatabaseNotReachedException(e);
				}
			} else {
				assert !wasReadLocked;
				assert this.sharedLocks.get() == 0;
			}
			try {
				this.checkIsZKLockable();
				lock.getExclusiveLock(lockTimeout);
			} catch (Exception e) {
				throw new DatabaseNotReachedException(e);
			}
		//}
		
		if (locks == null) {
			locks = new LinkedList<LockKind>();
			this.acquiredLocks.put(Thread.currentThread(), locks);
		}
		locks.addFirst(LockKind.EXCLUSIVE);
			
		assert ! this.isShareLocked();
		assert this.isExclusiveLocked();
	}
	
	public void exclusiveUnlockTable() {
		if (!this.intraLock.isWriteLockedByCurrentThread())
			throw new IllegalMonitorStateException("Thread " + Thread.currentThread() + " does not hold exclusive lock for table " + this.table + "; can't release");

		assert isExclusiveLocked();
		assert !this.isShareLocked();
		
		LinkedList<LockKind> locks = this.acquiredLocks.get(Thread.currentThread());
		// Checking that last acquired lock is an exclusive lock
		LockKind last = locks.peek();
		assert last != null;
		if (!LockKind.EXCLUSIVE.equals(last)) {
			throw new IllegalMonitorStateException("Last lock for table " + this.table + " for this thread is not an exclusive lock");
		}
		// Removing the last shared lock
		locks.poll();
		// If we hold another exclusive lock, returning
		if (locks.contains(LockKind.EXCLUSIVE))
			return;

		boolean shouldAcquireSharedLock;
		if (locks.isEmpty()) {
			shouldAcquireSharedLock = false;
			this.acquiredLocks.remove(Thread.currentThread());
		} else {
			shouldAcquireSharedLock = true;
			assert LockKind.SHARED.equals(locks.peek());
			assert this.sharedLocks.get() > 0;
		}

		//synchronized (this) { // Useless as we already own the internal write lock
			
			try {
				this.checkZooKeeper();
				lock.releaseExclusiveLock();
			} catch (Exception e) {
				try {
					this.checkZooKeeper();
					lock.releaseExclusiveLock();
				} catch (Exception f) {
					Store.errorLogger.log(Level.SEVERE, "Error unlocking table " + table + " locked in exclusion", e);
				}
			} finally {
				if (this.sharedLocks.get() > 0) {
					try {
						this.checkIsZKLockable();
						lock.getSharedLock(lockTimeout);
					} catch (Exception e) {
						try {
							this.checkIsZKLockable();
							lock.getSharedLock(lockTimeout);
						} catch (Exception f) {
							throw new DatabaseNotReachedException(e);
						}
					}
				}
			}
		//}
		assert ! this.isExclusiveLocked();
		
		this.intraLock.writeLock().unlock();
		if (shouldAcquireSharedLock) {
			this.intraLock.readLock().lock();
		}
	}

	public boolean isExclusiveLocked() {
		return lock.getCurrentExclusiveLock() != null;
	}

	public boolean isFree() {
		return !this.isExclusiveLocked() && !this.isShareLocked() && !Thread.holdsLock(this);
	}

}
