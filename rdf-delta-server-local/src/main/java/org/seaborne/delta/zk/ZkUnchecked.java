package org.seaborne.delta.zk;

import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import java.util.List;
import static org.seaborne.delta.server.local.DPS.LOG;

/**
 *
 */
public final class ZkUnchecked implements UncheckedZk {
    private final Zk decorated;

    public ZkUnchecked(final Zk decorated) {
        this.decorated = decorated;
    }

    @Override
    public boolean pathExists(final String path) {
        return wrapException(() -> this.decorated.pathExists(path));
    }

    @Override
    public String ensurePathExists(final String path) {
        return logAndHideException(() -> this.decorated.ensurePathExists(path), String.format("Failed: mkdirs(%s)", path));
    }

    @Override
    public byte[] fetch(String path) {
        return hideException(() -> this.decorated.fetch(path));
    }

    @Override
    public byte[] fetch(Watcher watcher, String path) {
        return hideException(() -> this.decorated.fetch(watcher, path));
    }

    @Override
    public JsonObject fetchJson(final String path) {
        return hideException(() -> this.decorated.fetchJson(path));
    }

    @Override
    public JsonObject fetchJson(final Watcher watcher, final String path) {
        return hideException(() -> this.decorated.fetchJson(watcher, path));
    }

    @Override
    public List<String> fetchChildren(final String path) {
        try {
            return this.decorated.fetchChildren(path);
        } catch (Exception e) {
            if ( e instanceof KeeperException.NoNodeException )
                LOG.error("No such znode: "+path);
            else
                LOG.error("Failed: zkSubNodes("+path+")",e);
            return null;
        }
    }

    @Override
    public List<String> fetchChildren(Watcher watcher, String path) {
        return hideException(() -> this.decorated.fetchChildren(watcher, path));
    }

    @Override
    public String createZNode(final String path) {
        return wrapException(() -> this.decorated.createZNode(path));
    }

    @Override
    public String createZNode(final String path, final CreateMode mode) {
        return wrapException(() ->decorated.createZNode(path, mode));
    }

    @Override
    public String createAndSetZNode(final String path, final JsonObject object) {
        return wrapException(() -> this.decorated.createAndSetZNode(path, object));
    }

    @Override
    public String createAndSetZNode(final String path, final byte[] bytes) {
        return wrapException(() -> this.decorated.createAndSetZNode(path, bytes));
    }

    @Override
    public void setZNode(final String path, final JsonObject object) {
        logAndHideException(() -> this.decorated.setZNode(path, object), String.format("Did not set: %s", path));
    }

    @Override
    public void setZNode(final String path, final byte[] bytes) {
        logAndHideException(() -> this.decorated.setZNode(path, bytes), String.format("Did not set: %s", path));
    }

    @Override
    public void deleteZNode(final String path) {
        wrapException(
            () -> {
                this.decorated.deleteZNode(path);
                return null;
            }
        );
    }

    @Override
    public InterProcessLock createLock(final String nLock) {
        return this.decorated.createLock(nLock);
    }

    @Override
    public void close() {
        hideException(
            () -> {
                this.decorated.close();
                return null;
            }
        );
    }

    @FunctionalInterface
    private interface CheckedFunc<T> {
        T run() throws Exception;
    }

    @FunctionalInterface
    private interface  CheckedProc {
        void run() throws Exception;
    }

    private <X> X wrapException(final CheckedFunc<X> action) {
        try {
            return action.run();
        } catch (Exception ex) {
            LOG.warn("ZooKeeper exception: "+ex.getMessage(), ex);
            throw new ZkException(ex.getMessage(), ex);
        }
    }

    private <X> X hideException(final CheckedFunc<X> action) {
        try {
            return action.run();
        } catch (Exception e) {
            return null;
        }
    }

    private <X> X logAndHideException(final CheckedFunc<X> action, final String logMessage) {
        try {
            return action.run();
        } catch (Exception e) {
            LOG.error(logMessage, e);
            return null;
        }
    }

    private void logAndHideException(final CheckedProc action, final String logMessage) {
        this.logAndHideException(
            () -> {
                action.run();
                return null;
            },
            logMessage
        );
    }
}
