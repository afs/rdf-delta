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

package org.seaborne.delta.zk;

import org.apache.jena.atlas.json.JsonObject;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import java.util.List;
import java.util.function.Supplier;

import static org.seaborne.delta.server.local.DPS.LOG;

/**
 * Wraps a {@link ZkConnection} and handles {@link Exception}s in its methods according to how they were handled
 * in the old Zk utility class. This ensures consistency of handling without requiring that the handling be duplicated
 * in every implementation of {@link ZkConnection}.
 */
public final class WrappedUncheckedZkConnection implements UncheckedZkConnection {
    /**
     * The underlying {@link ZkConnection}.
     */
    private final ZkConnection decorated;

    /**
     * Constructor.
     * @param decorated The {@link ZkConnection} to decorate.
     */
    public WrappedUncheckedZkConnection(final ZkConnection decorated) {
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
    public void deleteZNodeAndChildren(final String path) {
        wrapException(
            () -> {
                this.decorated.deleteZNodeAndChildren(path);
                return null;
            }
        );
    }

    @Override
    public void runWithLock(String path, Runnable action) {
        this.decorated.runWithLock(path, action);
    }

    @Override
    public <X> X runWithLock(String path, Supplier<X> action) {
        return this.decorated.runWithLock(path, action);
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
