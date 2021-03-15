package org.seaborne.delta.zk.curator;

import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.seaborne.delta.zk.ZkLock;

public final class CuratorZkLock implements ZkLock {
    private final InterProcessLock lock;

    CuratorZkLock(final InterProcessLock lock) {
        this.lock = lock;
    }

    @Override
    public void close() throws Exception {
        this.lock.release();
    }
}
