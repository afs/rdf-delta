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

import org.seaborne.delta.zk.ZkLock;

/**
 * A handle to a distributed lock in ZooKeeper.
 * {@see DirectZkLockFactory}
 */
public final class DirectZkLock implements ZkLock {
    /**
     * A ZooKeeper client connection.
     */
    private final ZooKeeperProvider client;

    /**
     * The ZooKeeper path to the lock.
     */
    private final String lock;

    /**
     * Constructs a new instance of {@link DirectZkLock} with the given client at the given lock path.
     * @param client A ZooKeeper client connection.
     * @param lock The ZooKeeper path to the lock.
     */
    public DirectZkLock(final ZooKeeperProvider client, final String lock) {
        this.client = client;
        this.lock = lock;
    }

    @Override
    public void close() throws Exception {
        this.client.zooKeeper().delete(this.lock, this.client.zooKeeper().exists(this.lock, false).getVersion());
    }
}
