package org.seaborne.delta.zk;

import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

import java.util.List;

public interface UncheckedZk extends Zk {
    @Override
    boolean pathExists(String path);

    @Override
    String ensurePathExists(String path);

    @Override
    byte[] fetch(String path);

    @Override
    byte[] fetch(Watcher watcher, String path);

    @Override
    JsonObject fetchJson(String path);

    @Override
    JsonObject fetchJson(Watcher watcher, String path);

    @Override
    List<String> fetchChildren(String path);

    @Override
    List<String> fetchChildren(Watcher watcher, String path);

    @Override
    String createZNode(String path);

    @Override
    String createZNode(String path, CreateMode mode);

    @Override
    String createAndSetZNode(String path, JsonObject object);

    @Override
    String createAndSetZNode(String path, byte[] bytes);

    @Override
    void setZNode(String path, JsonObject object);

    @Override
    void setZNode(String path, byte[] bytes);

    @Override
    void deleteZNode(String path);

    @Override
    InterProcessLock createLock(String nLock);

    @Override
    void close();
}
