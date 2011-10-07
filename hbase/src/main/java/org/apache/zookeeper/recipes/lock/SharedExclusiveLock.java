/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.recipes.lock;


import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class SharedExclusiveLock {
    /**
     * Shared lock node prefix.
     */
    private String SHLOCK = "shlock-";

    /**
     * Exclusive lock node prefix.
     */
    private String EXLOCK = "exlock-";

    private ZooKeeper zooKeeper;
    private String lockPath;
    private String writeLockPrefix;
    private String readLockPrefix;

    /**
     * When an exclusive lock is acquired, we write down ourselves as the owner
     * so we can not dead-lock ourselves.
     */
    private String currentExclusiveLock;

    private String currentSharedLock;

    public SharedExclusiveLock(ZooKeeper zooKeeper, String lockPath) {
        this.zooKeeper = zooKeeper;
        this.lockPath = lockPath;
        this.writeLockPrefix = lockPath + "/" + EXLOCK;
        this.readLockPrefix = lockPath + "/" + SHLOCK;
        this.currentExclusiveLock = null;
        this.currentSharedLock = null;
    }

    public String getPath() {
        return lockPath;
    }

    public boolean isExclusive(String lockNodePath) {
        return lockNodePath.startsWith(writeLockPrefix);
    }

    public boolean isShared(String lockNodePath) {
        return lockNodePath.startsWith(readLockPrefix);
    }

    public boolean isExpired(String lockNodePath) throws KeeperException,
            InterruptedException {
        return zooKeeper.exists(lockNodePath, false) == null;
    }

    public String getExclusiveLock() throws KeeperException,
            InterruptedException {
        return getExclusiveLock(0L);
    }

    public String getSharedLock() throws KeeperException, InterruptedException {
        return getSharedLock(0L);
    }
    
    /* ADDED by F.Fondement */
    public ZooKeeper getZookeeper() {
    	return this.zooKeeper;
    }

    
    /* ADDED by F.Fondement */
    public void setZookeeper(ZooKeeper zk) {
    	this.zooKeeper = zk;
    }

    /**
     * Block until we obtain an exclusive lock. Taken from
     * http://hadoop.apache.org/zookeeper/docs/r3.3.0/recipes.html#Shared+Locks
     *
     * @param timeout
     *            the timeout in milliseconds to wait for a lock or to abandon
     *            the prospect.
     * @return the lock name or null if this timeout.
     * @throws InterruptedException
     * @throws KeeperException
     */
    public String getExclusiveLock(Long timeout) throws KeeperException,
            InterruptedException {
        // Check that we don't already have a lock...
        if (currentExclusiveLock != null && !isExpired(currentExclusiveLock)) {
            // We have the exclusive lock! Remove newly made lock file and just
            // return.
            return currentExclusiveLock;
        }

        // Request a lock. This does not mean we proceed as if we held the lock.
        final String exclusiveLock = zooKeeper.create(writeLockPrefix, null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        final Long exclusiveLockId = getSequenceId(exclusiveLock);

        Long lockId;
        String lock = null;

        boolean checkForLocks = true;

        try {
        	while (checkForLocks) {
                        lockId = -1L;

        		List<String> children = zooKeeper.getChildren(lockPath, false);

        		for (String child : children) {
        			final Long tmp = getSequenceId(child);

        			if (tmp > lockId && tmp < exclusiveLockId) {
        				lockId = tmp;
        				lock = lockPath + "/" + child;
        			}
			}

       			// There is any other lock...
        		if (lockId >= 0) {
        			final Object mutex = new Object();

        			synchronized (mutex) {
        				final Stat lockStat = zooKeeper.exists(lock,
        						new NotifyOnDelete(mutex));

        				if (lockStat != null) {
        					long startTime = System.currentTimeMillis();

       						mutex.wait(timeout);

       						if (timeout > 0
       								&& System.currentTimeMillis() - startTime > timeout) {
       							zooKeeper.delete(exclusiveLock, -1);
       							return null;
       						}

       						// Notice: We recheck for new locks that may have
       						// shown up.
       						// Perhaps this is necessary? The cost is 1 extra
       								// getChildren
       						// call, so it is worth the protection.
       						checkForLocks = true;
       					} else {
       						// If we are here, then the file was possibly
       						// deleted / recreated.
       						// We need to recheck for write locks again...
       						checkForLocks = true;
      					}
       				}
       			} else {
       				// If we found NO other write lock, exit.
       				checkForLocks = false;
       			}
       		}
        } catch(InterruptedException interruptedException) {
        	// NOTE: This may be thrown by the mutex.wait or ZK operations. Both results in
        	//       roll back of our lock attempt.
        	zooKeeper.delete(exclusiveLock, -1);
        	throw interruptedException;
        } catch(KeeperException zookeeperException) {
        	zooKeeper.delete(exclusiveLock, -1);
        	throw zookeeperException;
        }

        currentExclusiveLock = exclusiveLock;

        return exclusiveLock;
    }

    /**
     * Block until we obtain a shared lock. Taken from
     * http://hadoop.apache.org/zookeeper/docs/r3.3.0/recipes.html#Shared+Locks
     *
     * @param timeout
     *            the timeout in milliseconds to wait for a lock or to abandon
     *            the prospect.
     * @return the lock name or null if this timeout.
     * @throws InterruptedException
     * @throws KeeperException
     */
    public String getSharedLock(Long timeout) throws KeeperException,
            InterruptedException {
        // Check that we don't already have a lock...
        if (currentSharedLock != null && !isExpired(currentSharedLock)) {
            // We have the exclusive lock! Remove newly made lock file and just
            // return.
            return currentSharedLock;
        }

        // Request a lock. This does not mean we proceed as if we held the lock.
        final String sharedLock = zooKeeper.create(readLockPrefix, null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        final Long sharedLockId = getSequenceId(sharedLock);

        Long exclusiveLockId;
        String exclusiveLock = null;

        boolean checkForExclusiveLocks = true;

        try {
	        while (checkForExclusiveLocks) {
                    exclusiveLockId = -1L;

	            List<String> children = zooKeeper.getChildren(lockPath, false);
	
	            for (String child : children) {
	                if (child.startsWith(EXLOCK)) {
	                    final Long tmp = getSequenceId(child);
	
	                    if (tmp > exclusiveLockId && tmp < sharedLockId) {
	                    	exclusiveLockId = tmp;
	                    	exclusiveLock = lockPath + "/" + child;
	                    }
	                }
	            }
	
	            // There is a write in progress. Wait for it...
	            if (exclusiveLockId >= 0) {
	                final Object mutex = new Object();
	
	                synchronized (mutex) {
	                    final Stat writeLockStat = zooKeeper.exists(exclusiveLock,
	                            new NotifyOnDelete(mutex));
	
	                    if (writeLockStat != null) {
	                        long startTime = System.currentTimeMillis();
	
	                        mutex.wait(timeout);
	
	                        if (timeout > 0
	                                && System.currentTimeMillis() - startTime > timeout) {
	                            zooKeeper.delete(sharedLock, -1);
	                            return null;
	                        }
	
	                        // Notice: We recheck for new locks that may have shown
	                        // up.
	                        // Perhaps this is necessary? The cost is 1 extra
	                        // getChildren call,
	                        // so it is worth the protection.
	                        checkForExclusiveLocks = true;
	                    } else {
	                        // If we are here, then the file was possibly deleted /
	                        // recreated.
	                        // We need to recheck for write locks again...
	                    	checkForExclusiveLocks = true;
	                    }
	                }
	            } else {
	                // If we found NO other write lock, exit.
	            	checkForExclusiveLocks = false;
	            }
	        }
        } catch(InterruptedException interruptedException) {
        	// NOTE: This may be thrown by the mutex.wait or ZK operations. Both results in
        	//       roll back of our lock attempt.
        	zooKeeper.delete(sharedLock, -1);
        	throw interruptedException;
        } catch(KeeperException zookeeperException) {
        	zooKeeper.delete(sharedLock, -1);
        	throw zookeeperException;
        }
        currentSharedLock = sharedLock;

        return sharedLock;
    }

    public void releaseSharedLock() throws InterruptedException,
            KeeperException {
        zooKeeper.delete(currentSharedLock, -1);
        currentSharedLock = null;
    }

    public void releaseExclusiveLock() throws InterruptedException,
            KeeperException {
        zooKeeper.delete(currentExclusiveLock, -1);
        currentExclusiveLock = null;
    }

    private Long getSequenceId(final String fileName) {
        final Matcher m = Pattern.compile(".*[^\\d](\\d+)").matcher(fileName);

        m.matches();

        return Long.parseLong(m.group(1));
    }

    public String getCurrentExclusiveLock() {
        return currentExclusiveLock;
    }

    public String getCurrentSharedLock() {
        return currentSharedLock;
    }

    /**
     * Callback object used to unblock caller threads waiting to recheck if they
     * have acuired a lock.
     */
    private static class NotifyOnDelete implements Watcher {
        private Object mutex;

        public NotifyOnDelete(Object mutex) {
            this.mutex = mutex;
        }

        @Override
        public void process(WatchedEvent event) {
            synchronized (mutex) {
                switch (event.getType()) {
                case NodeDeleted:
                    mutex.notify();
                    break;
                }
            }
        }
    }

}