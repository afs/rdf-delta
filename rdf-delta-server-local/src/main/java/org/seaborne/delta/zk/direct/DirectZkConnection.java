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

import org.apache.curator.utils.ZKPaths;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.zookeeper.*;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.zk.ZkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

public final class DirectZkConnection implements ZkConnection {
    private static final Logger LOG = LoggerFactory.getLogger(DirectZkConnection.class);

    public static DirectZkConnection connect(final String connectString) throws IOException, KeeperException, InterruptedException {
        return new DirectZkConnection(
            new ValidZooKeeperSupplier(connectString)
        );
    }

    private final DirectZkLockFactory lockFactory;

    private final ValidZooKeeperSupplier client;

    private DirectZkConnection(final ValidZooKeeperSupplier client) {
        this.client = client;
        this.lockFactory = new DirectZkLockFactory(client);
    }



    @Override
    public boolean pathExists(final String path) throws KeeperException, InterruptedException {
        LOG.debug("Checking if {} exists.", path);
        return this.client.get().exists(path, false) != null;
    }

    @Override
    public  final String ensurePathExists(final String path) throws KeeperException, InterruptedException {
        LOG.debug("Ensuring {} exists.", path);
        ZKPaths.mkdirs(this.client.get(), path, true);
        return path;
    }

    @Override
    public byte[] fetch(final String path) throws KeeperException, InterruptedException {
        LOG.debug("Fetching {}.", path);
        return this.client.get().getData(path, false, this.client.get().exists(path, false));
    }

    @Override
    public byte[] fetch(final Watcher watcher,  final String path) throws KeeperException, InterruptedException {
        LOG.debug("Fetching {}.", path);
        return this.client.get().getData(path, watcher, this.client.get().exists(path, false));
    }

    @Override
    public JsonObject fetchJson(final String path) throws Exception {
        return JSONX.fromBytes(this.fetch(path));
    }

    @Override
    public JsonObject fetchJson(final Watcher watcher,  final String path) throws Exception {
        return JSONX.fromBytes(this.fetch(watcher, path));
    }

    @Override
    public List<String> fetchChildren(final String path) throws KeeperException, InterruptedException {
        LOG.debug("Fetching the children of {}.", path);
        return this.client.get().getChildren(path, false);
    }

    @Override
    public List<String> fetchChildren(final Watcher watcher,  final String path) throws KeeperException, InterruptedException {
        LOG.debug("Fetching the children of {}.", path);
        return this.client.get().getChildren(path, watcher);
    }

    @Override
    public String createZNode(final String path) throws KeeperException, InterruptedException {
        return this.createZNode(path, CreateMode.PERSISTENT);
    }

    @Override
    public String createZNode(final String path, final CreateMode mode) throws KeeperException, InterruptedException {
        LOG.debug("Creating {}.", path);
        return this.client.get().create(
            path,
            new byte[0],
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            mode
        );
    }

    @Override
    public String createAndSetZNode(final String path, final JsonObject object) throws Exception {
        return this.createAndSetZNode(path, JSONX.asBytes(object));
    }

    @Override
    public String createAndSetZNode(final String path, byte[] bytes) throws KeeperException, InterruptedException {
        LOG.debug("Creating {} and setting it to {}.", path, new String(bytes));
        return this.client.get().create(
            path,
            bytes,
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT
        );
    }

    @Override
    public void setZNode(final String path, final JsonObject object) throws Exception {
        this.setZNode(path, JSONX.asBytes(object));
    }

    @Override
    public void setZNode(final String path, byte[] bytes) throws KeeperException, InterruptedException {
        LOG.debug("Setting {} to {}.", path, new String(bytes));
        this.client.get().setData(path, bytes, this.client.get().exists(path, false).getVersion());
    }

    @Override
    public void deleteZNodeAndChildren(final String path) throws KeeperException, InterruptedException {
        LOG.debug("Deleting {} and its children.", path);
        final Transaction transaction = this.client.get().transaction();
        this.deleteZNodeAndChildren(transaction, path);
        transaction.commit();
    }

    private void deleteZNodeAndChildren(final Transaction transaction, final String path) throws KeeperException, InterruptedException {
        for (final String child : this.client.get().getChildren(path, false)) {
            final String childPath = String.format("%s/%s", path, child);
            this.deleteZNodeAndChildren(transaction, childPath);
        }
        transaction.delete(path, this.client.get().exists(path, false).getVersion());
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
        try (var ignored = this.lockFactory.acquire(path)) {
            return action.get();
        } catch(DeltaException ex) {
            throw ex;
        } catch (InterruptedException e) {
            LOG.error("Interrupted while attempting to acquire a lock.", e);
        } catch (KeeperException e) {
            LOG.error("Error occurred while trying to acquire a lock.", e);
        } catch (Exception e) {
            LOG.error("Error performing the operation.", e);
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing ZooKeeper connection.");
        this.client.close();
    }
}
