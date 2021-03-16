package org.seaborne.delta.zk.direct;

import org.apache.zookeeper.ZooKeeper;
import org.seaborne.delta.zk.ZkLock;

public final class DirectZkLock implements ZkLock {
    private final ZooKeeper client;
    private final String lock;

    public DirectZkLock(final ZooKeeper client, final String lock) {
        this.client = client;
        this.lock = lock;
    }

    @Override
    public void close() throws Exception {
        this.client.delete(this.lock, this.client.exists(this.lock, false).getVersion());
    }
}
