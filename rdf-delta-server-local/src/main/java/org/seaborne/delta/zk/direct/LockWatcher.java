package org.seaborne.delta.zk.direct;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Watcher to monitor for the deletion of the predecessor to the current lock.
 */
public final class LockWatcher implements Watcher {
    @Override
    public void process(final WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
            synchronized(this) {
                this.notify();
            }
        }
    }
}
