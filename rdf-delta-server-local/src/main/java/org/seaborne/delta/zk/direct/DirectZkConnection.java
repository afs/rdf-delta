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
import org.apache.zookeeper.data.ACL;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.zk.ZkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public final class DirectZkConnection implements ZkConnection {
    private static final Logger LOG = LoggerFactory.getLogger(DirectZkConnection.class);

    private final ZooKeeper client;
    private final DirectZkLockFactory lockFactory;

    public static DirectZkConnection connect(final String connectString) throws IOException {
        return new DirectZkConnection(
            new ZooKeeper(connectString, 10_000, null)
        );
    }

    private DirectZkConnection(final ZooKeeper client) {
        this.client = client;
        this.lockFactory = new DirectZkLockFactory(client);
    }

    @Override
    public boolean pathExists(final String path) throws KeeperException, InterruptedException {
        return this.client.exists(path, false) != null;
    }

    @Override
    public  final String ensurePathExists(final String path) throws KeeperException, InterruptedException {
        ZKPaths.mkdirs(this.client, path, true);
        return path;
    }

    @Override
    public byte[] fetch(final String path) throws KeeperException, InterruptedException {
        return this.client.getData(path, false, this.client.exists(path, false));
    }

    @Override
    public byte[] fetch(final Watcher watcher,  final String path) throws KeeperException, InterruptedException {
        return this.client.getData(path, watcher, this.client.exists(path, false));
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
        return this.client.getChildren(path, false);
    }

    @Override
    public List<String> fetchChildren(final Watcher watcher,  final String path) throws KeeperException, InterruptedException {
        return this.client.getChildren(path, watcher);
    }

    @Override
    public String createZNode(final String path) throws KeeperException, InterruptedException {
        return this.createZNode(path, CreateMode.PERSISTENT);
    }

    @Override
    public String createZNode(final String path, CreateMode mode) throws KeeperException, InterruptedException {
        return this.client.create(
            path,
            new byte[0],
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            mode
        );
    }

    @Override
    public String createAndSetZNode(final String path, JsonObject object) throws Exception {
        return this.createAndSetZNode(path, JSONX.asBytes(object));
    }

    @Override
    public String createAndSetZNode(final String path, byte[] bytes) throws KeeperException, InterruptedException {
        return this.client.create(
            path,
            bytes,
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT
        );
    }

    @Override
    public void setZNode(final String path, JsonObject object) throws Exception {
        this.setZNode(path, JSONX.asBytes(object));
    }

    @Override
    public void setZNode( final String path, byte[] bytes) throws KeeperException, InterruptedException {
        this.client.setData(path, bytes, this.client.exists(path, false).getVersion());
    }

    @Override
    public void deleteZNode(final String path) throws KeeperException, InterruptedException {
        this.client.delete(path, this.client.exists(path, false).getVersion());
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
        this.client.close();
    }
}
