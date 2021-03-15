package org.seaborne.delta.zk;

public interface ZkLockFactory {
    ZkLock acquire(String logPath) throws Exception;
}
