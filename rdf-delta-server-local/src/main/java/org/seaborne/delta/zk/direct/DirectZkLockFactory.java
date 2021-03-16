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
import org.apache.zookeeper.data.ACL;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Acquires distributed locks.
 */
public final class DirectZkLockFactory {
    /**
     * ZooKeeper connection.
     */
    private final ZooKeeper client;

    /**
     * Constructs an instance from a ZooKeeper connection.
     * @param client Zookeeper connection.
     */
    public DirectZkLockFactory(final ZooKeeper client) {
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
        final String lockPath = this.client.create(
            String.format("%s/%s", path, "directZkLock"),
            new byte[0],
            List.of(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE)),
            CreateMode.EPHEMERAL_SEQUENTIAL
        );
        final String lockNodeName = lockPath.replace(String.format("%s/", path), "");
        final Optional<String> predecessor = this.client.getChildren(path, false).stream()
            .filter(x -> x.compareTo(lockNodeName) < 0)
            .max(Comparator.naturalOrder());
        if (predecessor.isPresent()) {
            final Watcher watcher = new LockWatcher();
            this.client.addWatch(
                String.format("%s/%s", path, predecessor.get()),
                watcher,
                AddWatchMode.PERSISTENT
            );
            watcher.wait();
            this.client.removeWatches(predecessor.get(), watcher, Watcher.WatcherType.Any, true);
        }
        return new DirectZkLock(this.client, lockPath);
    }
}
