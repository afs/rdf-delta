package org.seaborne.delta.zk;

import org.apache.jena.atlas.json.JsonObject;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

import java.util.List;
import java.util.function.Supplier;

public interface ZkConnection extends AutoCloseable {
    boolean pathExists(String path) throws Exception;

    String ensurePathExists(String path) throws Exception;

    byte[] fetch(String path) throws Exception;

    byte[] fetch(Watcher watcher, String path) throws Exception;

    JsonObject fetchJson(String path) throws Exception;

    JsonObject fetchJson(Watcher watcher, String path) throws Exception;

    List<String> fetchChildren(String path) throws Exception;

    List<String> fetchChildren(Watcher watcher, String path) throws Exception;

    String createZNode(String path) throws Exception;

    String createZNode(String path, CreateMode mode) throws Exception;

    String createAndSetZNode(String path, JsonObject object) throws Exception;

    String createAndSetZNode(String path, byte[] bytes) throws Exception;

    void setZNode(String path, JsonObject object) throws Exception;

    void setZNode(String path, byte[] bytes) throws Exception;

    void deleteZNode(String path) throws Exception;

    ZkLock acquireLock(String path) throws Exception;

    void runWithLock(String path, Runnable action);

    <X> X runWithLock(String path, Supplier<X> action);
}
