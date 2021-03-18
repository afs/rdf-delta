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

import org.apache.zookeeper.*;
import org.apache.zookeeper.server.util.ConfigUtils;
import org.seaborne.delta.zk.ZkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class ValidZooKeeperSupplier implements Supplier<ZooKeeper>, Watcher, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ValidZooKeeperSupplier.class);
    private final Object token = new Object();
    private final int retries;
    private CharSequence connectString;
    private ZooKeeper zooKeeper;
    private boolean isValid = false;

    public ValidZooKeeperSupplier(final CharSequence connectString) throws IOException, KeeperException, InterruptedException {
        this(connectString, 5);
    }

    public ValidZooKeeperSupplier(final CharSequence connectString, final int retries) throws IOException, KeeperException, InterruptedException {
        this.connectString = connectString;
        this.retries = retries;
        this.connect();
    }

    private void connect() throws IOException, KeeperException, InterruptedException {
        this.zooKeeper = new ZooKeeper(
            this.connectString.toString(),
            10_000,
            watchedEvent -> {
                synchronized (this.token) {
                    switch (watchedEvent.getState()) {
                        case SyncConnected:
                        case SaslAuthenticated:
                        case ConnectedReadOnly:
                            this.isValid = true;
                            break;
                        case Closed:
                        case Expired:
                        case AuthFailed:
                        case Disconnected:
                            this.isValid = false;
                    }
                    this.token.notifyAll();
                }
            }
        );
        this.updateConfig();
    }

    @Override
    public ZooKeeper get() {
        try {
            long tries = 1;
            synchronized (this.token) {
                while (!this.isValid) {
                    LOG.info("Connection flagged as invalid with state: {}", this.zooKeeper.getState());
                    switch (this.zooKeeper.getState()) {
                        case CONNECTING:
                            LOG.info("Waiting...");
                            this.token.wait(3000);
                            break;
                        case AUTH_FAILED:
                            throw new ZkException("Authentication failed.");
                        case CLOSED:
                        case NOT_CONNECTED:
                            try {
                                LOG.info("Attempting to reconnect...");
                                this.connect();
                            } catch (final IOException | KeeperException e) {
                                LOG.error("Unable to connect to the ZooKeeper Ensemble.", e);
                            }
                        case CONNECTED:
                        case ASSOCIATING:
                        case CONNECTEDREADONLY:
                            LOG.info("Misflagged. Marking the connection as valid.");
                            this.isValid = true;
                    }
                    if (tries == this.retries) {
                        throw new ZkException(
                            String.format(
                                "Failed after %d attempts to connect to the ZooKeeper Ensemble.",
                                this.retries
                            )
                        );
                    } else {
                        ++tries;
                    }
                }
            }
            return this.zooKeeper;
        } catch (final InterruptedException e) {
            throw new ZkException("Interrupted while attempting to connect to the ZooKeeper Ensemble.", e);
        }
    }

    private void updateConfig() throws KeeperException, InterruptedException, IOException {
        final byte[] newConfig = this.get().getConfig(
            this,
            this.get().exists(
                ZooDefs.CONFIG_NODE,
                false
            )
        );
        if (newConfig.length > 0) {
            synchronized (this.token) {
                this.connectString = Arrays.stream(new String(newConfig).split("\n"))
                    .filter(s -> s.startsWith("server"))
                    .map(s -> s.split("=")[1])
                    .map(
                        s -> {
                            var elements = s.split(":");
                            return String.format("%s:%s", elements[0], elements[elements.length - 1]);
                        }
                    ).collect(Collectors.joining(","));
                LOG.info("Setting the connectString to {}", this.connectString);
                this.get().updateServerList(this.connectString.toString());
                do {
                    this.token.wait(5000);
                } while (!this.isValid);
            }
        }
    }

    @Override
    public void process(final WatchedEvent event) {
        if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
            try {
                LOG.info("New config received.");
                this.updateConfig();
            } catch (final KeeperException | InterruptedException e) {
                LOG.error("Failure retrieving the updated ZooKeeper config.", e);
            } catch (IOException e) {
                throw new ZkException("Failure updating the ZooKeeper config.", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        this.zooKeeper.close();
    }
}
