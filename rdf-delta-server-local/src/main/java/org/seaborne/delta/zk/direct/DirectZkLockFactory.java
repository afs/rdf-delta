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

import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Acquires distributed locks.
 */
public final class DirectZkLockFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DirectZkLockFactory.class);

    /**
     * ZooKeeper connection.
     */
    private final Supplier<ZooKeeper> client;

    /**
     * Constructs an instance from a ZooKeeper connection.
     * @param client Zookeeper connection.
     */
    public DirectZkLockFactory(final Supplier<ZooKeeper> client) {
        this.client = client;
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
        final Optional<String> predecessor = this.client.get().getChildren(path, false).stream()
            .filter(x -> x.compareTo(lockNodeName) < 0)
            .max(Comparator.naturalOrder());
        LOG.debug("Will wait? {}", predecessor.isPresent());
        if (predecessor.isPresent()) {
            LOG.debug("Setting a watcher on predecessor: {}", predecessor.get());
            final LockWatcher watcher = new LockWatcher();
            this.client.get().addWatch(
                String.format("%s/%s", path, predecessor.get()),
                watcher,
                AddWatchMode.PERSISTENT
            );
            synchronized (watcher) {
                do {
                    LOG.debug("Going to sleep.");
                    watcher.wait();
                    LOG.debug("Waking up.");
                } while (!watcher.isLockAcquired());
            }
            LOG.debug("Cleaning up the watcher.");
            this.client.get().removeWatches(predecessor.get(), watcher, Watcher.WatcherType.Any, true);
        }
        return new DirectZkLock(this.client, lockPath);
    }
}
