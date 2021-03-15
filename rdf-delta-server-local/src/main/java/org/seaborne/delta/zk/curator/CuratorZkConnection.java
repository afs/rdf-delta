package org.seaborne.delta.zk.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.zk.ZkConnection;
import org.seaborne.delta.zk.ZkLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Supplier;

/**
 * This is an Object-Oriented implementation of the previous Zk utility class. This class is being introduced
 * to replicate the functionality of the previous design in transition to a more reliable implementation that uses
 * either a heavily patched Curator or discards Curator altogether.
 */
public final class CuratorZkConnection implements ZkConnection {
    private static final Logger LOG = LoggerFactory.getLogger(CuratorZkConnection.class);

    /**
     * Encapsulated {@link CuratorFramework}.
     * This is intended to live inside the boundaries of a blackbox. Therefore, it is not exposed to the outside world
     * with a getter or a setter.
     */
    private final CuratorFramework curator;

    /**
     * Static factory method that constructs and connects a ZkCurator.
     * @param connectString The string to bootstrap the connection with.
     * @return An instantiated and started ZkCurator.
     */
    public static CuratorZkConnection connect(final String connectString) {
        final var curator = new CuratorZkConnection(
            CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .ensembleTracker(true)
                .sessionTimeoutMs(10000)
                .connectionTimeoutMs(10000)
                .retryPolicy(
                    new ExponentialBackoffRetry(
                        10000,
                        5
                    )
                ).build()
        );
        curator.connect();
        return curator;
    }

    /**
     * Private constructor used by the factory method for instantiation.
     * @param curator The {@link CuratorFramework} to encapsulate in the resulting object.
     */
    private CuratorZkConnection(final CuratorFramework curator) {
        this.curator = curator;
    }

    /**
     * Internal method used to start this instance after construction in the factory method.
     * There is no need to guard against the status since this method is guaranteed to always be operating on a fresh
     * instance of {@link CuratorFramework}.
     */
    private void connect() {
        this.curator.start();
        try {
            this.curator.blockUntilConnected();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean pathExists(final String path) throws Exception {
        return this.curator.checkExists().forPath(path) != null;
    }

    @Override
    public String ensurePathExists(final String path) throws Exception {
        ZKPaths.mkdirs(this.curator.getZookeeperClient().getZooKeeper(), path, true);
        return path;
    }

    @Override
    public byte[] fetch(final String path) throws Exception {
        return this.fetch(null, path);
    }

    @Override
    public byte[] fetch(final Watcher watcher, final String path) throws Exception {
        final GetDataBuilder b = this.curator.getData();
        if ( watcher != null ) {
            b.usingWatcher(watcher);
        }
        return b.forPath(path);
    }

    @Override
    public JsonObject fetchJson(final String path) throws Exception {
        return this.fetchJson(null, path);
    }

    @Override
    public JsonObject fetchJson(final Watcher watcher, final String path) throws Exception {
        return JSONX.fromBytes(this.fetch(watcher, path));
    }

    @Override
    public List<String> fetchChildren(final String path) throws Exception {
        return this.fetchChildren(null, path);
    }

    @Override
    public List<String> fetchChildren(Watcher watcher, String path) throws Exception {
        final GetChildrenBuilder b = this.curator.getChildren();
        if (watcher != null) {
            b.usingWatcher(watcher);
        }
        return b.forPath(path);
    }

    @Override
    public String createZNode(final String path) throws Exception {
        return this.createZNode(path, CreateMode.PERSISTENT);
    }

    @Override
    public String createZNode(final String path, final CreateMode mode) throws Exception {
        return this.createAndSetZNode(path, new byte[0]);
    }

    @Override
    public String createAndSetZNode(final String path, final JsonObject object) throws Exception {
        return this.createAndSetZNode(path, JSONX.asBytes(object));
    }

    @Override
    public String createAndSetZNode(final String path, final byte[] bytes) throws Exception {
        return this.curator.create().forPath(path, bytes);
    }

    @Override
    public void setZNode(final String path, final JsonObject object) throws Exception {
        this.setZNode(path, JSONX.asBytes(object));
    }

    @Override
    public void setZNode(final String path, final byte[] bytes) throws Exception {
        if (this.curator.setData().forPath(path, bytes) == null) {
            throw new Exception();
        }
    }

    @Override
    public void deleteZNode(final String path) throws Exception {
        this.curator.delete().deletingChildrenIfNeeded().forPath(path);
    }

    @Override
    public ZkLock acquireLock(final String path) throws Exception {
        final InterProcessLock lock = new InterProcessSemaphoreMutex(this.curator, path);
        lock.acquire();
        if ( ! lock.isAcquiredInThisProcess() ) {
            LOG.warn("zkLock: lock.isAcquiredInThisProcess() is false");
        }
        return new CuratorZkLock(lock);
    }

    @Override
    public void runWithLock(final String path, final Runnable action) {
        this.runWithLock(path, () -> {
            action.run();
            return null;
        });
    }

    @Override
    public <X> X runWithLock(final String path, final Supplier<X> action) {
        try (var ignored = this.acquireLock(path)) {
            return action.get();
        } catch(DeltaException ex) {
            throw ex;
        } catch(RuntimeException ex) {
            FmtLog.warn(LOG, "RuntimeException in runWithLock");
            ex.printStackTrace();
            throw ex;
        } catch (Exception ignore) {
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        this.curator.close();
    }
}
