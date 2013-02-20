package com.googlecode.n_orm.hbase;

import java.util.logging.Level;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.recipes.lock.SharedExclusiveLock;

import com.googlecode.n_orm.DatabaseNotReachedException;

/**
 * A zookeeper-based read write lock for protecting on table schemas operations.
 * Read lock can be promoted to write lock ;
 * in this latter case write unlock brings back read lock.
 */
public class TableLocker {
	public static final long lockTimeout = 10000;
	
	private final Store store;
	private final String table;
	
	/**
	 * The actual zookeeper lock.
	 */
	private final SharedExclusiveLock lock;
	
	private volatile boolean wasPromotedFromSharedToToExclusive = false;
	
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
	 * Triggers a zookeeper query.
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
	
	public void sharedLockTable() throws DatabaseNotReachedException {
		synchronized (this) {
			assert lock.getCurrentExclusiveLock() == null;
			try {
				this.checkIsZKLockable();
				lock.getSharedLock(lockTimeout);
			} catch (Exception e) {
				throw new DatabaseNotReachedException(e);
			}
		}
	}
	
	public void sharedUnlockTable() throws DatabaseNotReachedException {
		synchronized (this) {
			if (isShareLocked()) {
				try {
					this.checkZooKeeper();
					lock.releaseSharedLock();
					this.wasPromotedFromSharedToToExclusive = false;
					assert lock.getCurrentExclusiveLock() == null;
				} catch (Exception e) {
					Store.errorLogger.log(Level.SEVERE, "Error unlocking table " + table, e);
				}
			}
		}
	}

	public boolean isShareLocked() {
		return lock.getCurrentSharedLock() != null;
	}
	
	public void exclusiveLockTable() throws DatabaseNotReachedException {
		synchronized (this) {
			if (isShareLocked()) {
				this.sharedUnlockTable();
				this.wasPromotedFromSharedToToExclusive = true;
			}
			try {
				this.checkIsZKLockable();
				lock.getExclusiveLock(lockTimeout);
				assert lock.getCurrentThread() == Thread.currentThread();
			} catch (Exception e) {
				throw new DatabaseNotReachedException(e);
			}
		}
	}
	
	public void exclusiveUnlockTable() {
		synchronized (this) {
			try {
				if (isExclusiveLocked()) {
					this.checkZooKeeper();
					lock.releaseExclusiveLock();
				}
			} catch (Exception e) {
				Store.errorLogger.log(Level.SEVERE, "Error unlocking table " + table + " locked in exclusion", e);
			} finally {
				if (this.wasPromotedFromSharedToToExclusive) {
					this.wasPromotedFromSharedToToExclusive = false;
					this.sharedLockTable();
				}
			}
		}
	}

	public boolean isExclusiveLocked() {
		return lock.getCurrentExclusiveLock() != null;
	}

	public boolean isFree() {
		return !this.isExclusiveLocked() && !this.isShareLocked() && !Thread.holdsLock(this);
	}

}
