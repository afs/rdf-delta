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
    private final Supplier<ZooKeeper> client;

    /**
     * The amount of time to wait to acquire the lock in milliseconds.
     */
    private final long timeout;

    /**
     * Token used to synchronize actions between {@link #acquire(String)} and {@link LockAcquiredWatcher}.
     */
    private final Object token = new Object();

    private final Map<String, Boolean> predecessors;

    /**
     * Constructs an instance from a ZooKeeper connection with a timeout of 10000 milliseconds.
     * @param client Zookeeper connection.
     */
    public DirectZkLockFactory(final Supplier<ZooKeeper> client) {
        this(client, 10000);
    }

    /**
     * Constructs an instance from a ZooKeeper connection.
     * @param client Zookeeper connection.
     * @param timeout The amount of time to wait to acquire the lock in milliseconds.
     */
    public DirectZkLockFactory(final Supplier<ZooKeeper> client, final long timeout) {
        this.client = client;
        this.timeout = timeout;
        this.predecessors = new HashMap<>(0);
    }

    private boolean isLockAcquired() {
        final var isAcquired = this.predecessors.values().stream().reduce(Boolean::logicalAnd);
        return isAcquired.isPresent() && isAcquired.get();
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
        final String lockPath = this.client.get().create(
            String.format("%s/%s", path, "directZkLock"),
            new byte[0],
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL
        );
        LOG.debug("LockPath: {}", lockPath);
        final String lockNodeName = lockPath.replace(String.format("%s/", path), "");
        LOG.debug("LockNodeName: {}", lockNodeName);
        final LockAcquiredWatcher watcher = new LockAcquiredWatcher(
            this.token,
            predecessor -> this.predecessors.put(predecessor, true),
            this::isLockAcquired
        );
        // Synchronizing on the token here to ensure consistent reads from isLockAcquired()
        synchronized (this.token) {
            this.client.get().getChildren(path, watcher).stream()
                .filter(x -> x.compareTo(lockNodeName) < 0)
                .forEach(key -> this.predecessors.put(key, false));
            if (!this.isLockAcquired()) {
                // Waiting here yields the synchronization lock to allow the watcher to update the state used in the
                // isLockAcquired() calculation. Doing it this way ensures consistent reads from isLockAcquired() since
                // even if the timeout is reached, the code must obtain a synchronization lock before proceeding
                // which allows any ongoing watcher update to finish before this code resumes.
                watcher.wait(this.timeout);
                if (!this.isLockAcquired()) {
                    throw new ZkException(
                        String.format("Failed to acquire the lock after %d milliseconds.", this.timeout)
                    );
                }
            }
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
                // This synchronization ensures consistent reads of isLockAcquired() by preventing the state of the
                // locks from being updated while the synchronization block starting at 109 has control.
                synchronized(this.token) {
                    LOG.info("Predecessor lock {} has been released.", watchedEvent.getPath());
                    this.onLockReleased.accept(watchedEvent.getPath());
                    if (this.isLockAcquired.get()) {
                        LOG.info("The lock is acquired.");
                        // This wakes the thread waiting at 118 above.
                        this.token.notifyAll();
                    }
                }
            }
        }
    }
}
