/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.zk.direct;

import org.apache.zookeeper.*;
import org.seaborne.delta.zk.ZkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Acquires distributed locks from ZooKeeper.
 *
 * Locks in ZooKeeper rely on ephemeral sequential nodes which act as a sort of primary key that is automatically
 * deleted in the event the client that created it disconnects. This prevents clients from erroneously holding locks
 * after they die. The sequential nature of these nodes is used as a kind of queue to order who gets the lock when.
 * Essentially, a process seeking to acquire a lock creates a node and waits for all of its predecessors to release
 * locks to proceed.
 * @see LockAcquiredWatcher
 */
public final class DirectZkLockFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DirectZkLockFactory.class);

    /**
     * ZooKeeper connection.
     */
    private final ZooKeeperProvider client;

    /**
     * The amount of time to wait to acquire the lock in milliseconds.
     */
    private final long timeout;

    /**
     * Constructs an instance from a ZooKeeper connection with a timeout of 10000 milliseconds.
     * @param client Zookeeper connection.
     */
    public DirectZkLockFactory(final ZooKeeperProvider client) {
        this(client, 10000);
    }

    /**
     * Constructs an instance from a ZooKeeper connection.
     * @param client Zookeeper connection.
     * @param timeout The amount of time to wait to acquire the lock in milliseconds.
     */
    public DirectZkLockFactory(final ZooKeeperProvider client, final long timeout) {
        this.client = client;
        this.timeout = timeout;
    }

    /**
     * Acquires a distributed lock. This method blocks until the lock is acquired and returns a handle to the lock
     * the receivers use to release it when they are done with it.
     * @param path The path at which to create the lock.
     * @return A handle to the lock.
     * @throws KeeperException if the server signals an error with a non-zero error code.
     * @throws InterruptedException if the server transaction is interrupted.
     */
    public DirectZkLock acquire(final String path) throws KeeperException, InterruptedException {
        LOG.debug("Path: {}", path);
        // Creates the ephemeral sequential node that represents the lock we're trying to acquire
        final String lockPath = this.client.zooKeeper().create(
            String.format("%s/%s", path, "directZkLock"),
            new byte[0],
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL
        );
        LOG.debug("LockPath: {}", lockPath);
        final String lockNodeName = lockPath.replace(String.format("%s/", path), "");
        LOG.debug("LockNodeName: {}", lockNodeName);
        // Token used to synchronize with LockAcquiredWatcher.
        final Object token = new Object();
        // The key is the path to a lock node that is a predecessor to the lock we're trying to acquire.
        // The value answers the question, "has this lock been released?"
        final Map<String, Boolean> predecessors= new HashMap<>(0);
        // Answers the question, "have all the predecessor locks been released?"
        final Supplier<Boolean> isLockAcquired = () -> predecessors.values().stream()
            .reduce(Boolean::logicalAnd)
            .orElse(true); // No values indicates no predecessor locks; therefore, true
        // Updates the state of the predecessors when their nodes are deleted
        final LockAcquiredWatcher watcher = new LockAcquiredWatcher(
            token,
            predecessor -> {
                if (predecessors.containsKey(predecessor)) {
                    predecessors.put(predecessor, true);
                }
            },
            isLockAcquired
        );
        // Synchronizing on the token here to ensure consistent reads from isLockAcquired.
        // We are synchronizing on a local variable because all the actions that need synchronization are completed
        // before this method exits.
        synchronized (token) {
            // Add a permanent watch at the path. If we add it in the getChildren() call below, it will clear the first
            // time it is triggered preventing us from receiving future notifications.
            this.client.zooKeeper().addWatch(path, watcher, AddWatchMode.PERSISTENT);
            // Here we get all lock nodes at the given path. We then put each lock that precedes the one we made above
            // into the predecessors map with an initial value of false, indicating that they haven't released their
            // locks yet. Being synchronized, we can be sure any predecessors that may release their locks between when
            // we query for locks and when they are put into the map will be updated once we wait on the token below.
            this.client.zooKeeper().getChildren(path, false).stream()
                .filter(lock -> lock.compareTo(lockNodeName) < 0)
                .forEach(lock -> predecessors.put(lock, false));
            if (!isLockAcquired.get()) {
                // Waiting here yields the synchronization lock to allow the watcher to update the state used in the
                // isLockAcquired calculation. Doing it this way ensures consistent reads from isLockAcquired since
                // even if the timeout is reached, the code must reobtain the synchronization lock before proceeding
                // which allows any ongoing watcher update to finish before this code resumes. We're ignoring the risk
                // of a spurious wakeup because the relative simplicity of treating it like a timeout outweighs the
                // benefit of looping and trying to track the timeout independently.
                token.wait(this.timeout);
                if (!isLockAcquired.get()) {
                    throw new ZkException(
                        String.format("Failed to acquire the lock after %d milliseconds.", this.timeout)
                    );
                }
            }
            // Clear the watch as it is no longer useful.
            this.client.zooKeeper().removeAllWatches(path, Watcher.WatcherType.Any, true);
        }
        return new DirectZkLock(this.client, lockPath);
    }

    /**
     * Watcher to monitor for the acquisition of a lock.
     *
     * A lock is represented by an ephemeral sequential node. The lock is said to be acquired when all predecessor nodes
     * in the sequence have been deleted.
     *
     * This {@link Watcher} is meant to be applied to all predecessor locks to signal through the {@link #notify()}
     * mechanism that code waiting to acquire a lock has acquired the lock and can now proceed. This mechanism is used
     * to bridge the inherently asynchronous {@link Watcher} mechanism with the inherently synchronous locking calls.
     * @see DirectZkLockFactory
     */
    private static final class LockAcquiredWatcher implements Watcher {
        /**
         * Logger.
         */
        private static final Logger LOG = LoggerFactory.getLogger(LockAcquiredWatcher.class);

        /**
         * Synchronization token used to notify waiting code when a lock is acquired.
         */
        private final Object token;

        /**
         * Event handler for when a lock is released.
         */
        private final Consumer<String> onLockReleased;

        /**
         * Checks if the lock being waited for has been acquired.
         */
        private final Supplier<Boolean> isLockAcquired;

        /**
         * Constructs a {@link LockAcquiredWatcher} with the given onLockReleased handler and isLockAcquired method..
         * @param token Synchronization token used to notify waiting code when a lock is acquired.
         * @param onLockReleased Event handler for when a lock is released.
         * @param isLockAcquired Checks if the lock being waited for has been acquired.
         */
        public LockAcquiredWatcher(
            final Object token,
            final Consumer<String> onLockReleased,
            final Supplier<Boolean> isLockAcquired
        ) {
            this.token = token;
            this.onLockReleased = onLockReleased;
            this.isLockAcquired = isLockAcquired;
        }

        @Override
        public void process(final WatchedEvent watchedEvent) {
            LOG.debug("Event received: {}", watchedEvent.getType());
            if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                // This synchronization ensures consistent reads of isLockAcquired by preventing the state of the
                // locks from being updated while the synchronization block in the acquired method has control.
                synchronized(this.token) {
                    LOG.info("Predecessor lock {} has been released.", watchedEvent.getPath());
                    this.onLockReleased.accept(watchedEvent.getPath());
                    if (this.isLockAcquired.get()) {
                        LOG.info("The lock is acquired.");
                        // This wakes the thread waiting in the acquired method.
                        this.token.notifyAll();
                    }
                }
            }
        }
    }
}
