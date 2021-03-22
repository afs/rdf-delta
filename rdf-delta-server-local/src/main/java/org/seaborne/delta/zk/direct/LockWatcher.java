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

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watcher to monitor for the deletion of the predecessor to the current lock.
 */
public final class LockWatcher implements Watcher {
    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(LockWatcher.class);

    /**
     * Is the lock acquired?
     */
    private boolean lockAcquired = false;

    @Override
    public void process(final WatchedEvent watchedEvent) {
        LOG.debug("Event received: {}", watchedEvent.getType());
        if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
            synchronized(this) {
                LOG.debug("Signaling that the lock is acquired.");
                this.lockAcquired = true;
                this.notifyAll();
            }
        }
    }

    /**
     * Is the lock acquired?
     * @return true if yes, false if no.
     */
    public boolean isLockAcquired() {
        return this.lockAcquired;
    }
}
