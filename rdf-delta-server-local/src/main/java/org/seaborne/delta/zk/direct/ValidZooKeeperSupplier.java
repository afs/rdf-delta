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
import org.seaborne.delta.zk.ZkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Supplies valid, connected {@link ZooKeeper} instances or throws an exception.
 *
 * <p>
 * {@link ZooKeeper} instances will attempt to reconnect on their own if the connection is lost. However, session
 * expiration causes an instance to become unusable. This class monitors connection state changes and will instantiate
 * a fresh {@link ZooKeeper} instance when a session expires. This class also monitors the Ensemble configuration and
 * will update the {@link #connectString} to reflect dynamic changes to the Ensemble.
 * </p>
 * <p>
 * If all the Ensemble members present in the configuration leave the ensemble while the {@link ZooKeeper} instance
 * is invalid, new connection attempts will fail resulting in a {@link ZkException} that will crash the application.
 * This crash is essential to enabling the detection and replacement of a failed RDF Delta node.
 * </p>
 * <p>
 * If {@link #close()} is called, calls to {@link #get()} will yield a {@link ZkException} that should crash the
 * system. Only call {@link #close()} when you are done with this {@link ValidZooKeeperSupplier}.
 * </p>
 */
public final class ValidZooKeeperSupplier implements Supplier<ZooKeeper>, Watcher, AutoCloseable {
    /**
     * {@link Logger}.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ValidZooKeeperSupplier.class);

    /**
     * {@link Object} used for synchronizing asynchronous callbacks.
     */
    private final Object token = new Object();

    /**
     * The number of times to retry reconnect attempts before throwing a {@link ZkException}.
     */
    private final int retries;

    /**
     * A comma-separate list of host:port pairs pointing to {@link ZooKeeper} Ensemble members.
     */
    private volatile CharSequence connectString;

    /**
     * The current {@link ZooKeeper} instance.
     */
    private volatile ZooKeeper zooKeeper;

    /**
     * The current validity of the connection as determined by the connection monitor.
     */
    private volatile boolean isValid = false;

    /**
     * Indicates whether {@link #close()} has been called.
     */
    private volatile boolean isClosed = false;

    /**
     * Instantiates a {@link ValidZooKeeperSupplier} with the given connectString specifying a default retry limit of 5.
     * @param connectString A comma-separated list of host:port pairs pointing to ZooKeeper Ensemble members.
     * @throws IOException if a problem occurs while attempting to connect to the ZooKeeper Ensemble.
     * @throws KeeperException if a problem occurs getting the current ZooKeeper Ensemble configuration.
     * @throws InterruptedException if the current thread is interrupted while waiting.
     */
    public ValidZooKeeperSupplier(final CharSequence connectString) throws IOException, KeeperException, InterruptedException {
        this(connectString, 5);
    }

    /**
     * Instantiates a {@link ValidZooKeeperSupplier} with the given connectString and the given retry limit.
     * @param connectString A comma-separated list of host:port pairs pointing to ZooKeeper Ensemble members.
     * @param retries The number of times to try to connect to a ZooKeeper Ensemble before giving up.
     * @throws IOException if a problem occurs while attempting to connect to the ZooKeeper Ensemble.
     * @throws KeeperException if a problem occurs getting the current ZooKeeper Ensemble configuration.
     * @throws InterruptedException if the current thread is interrupted while waiting.
     */
    public ValidZooKeeperSupplier(final CharSequence connectString, final int retries) throws IOException, KeeperException, InterruptedException {
        this.connectString = connectString;
        this.retries = retries;
        this.connect();
    }

    /**
     * Instantiates a new {@link ZooKeeper} instance and updates the Ensemble configuration.
     * @throws IOException if a problem occurs while attempting to connect to the ZooKeeper Ensemble.
     * @throws KeeperException if a problem occurs getting the current ZooKeeper Ensemble configuration.
     * @throws InterruptedException if the current thread is interrupted while waiting.
     */
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
                    if (this.isClosed) {
                        throw new ZkException("ValidZooKeeperSupplier instance has been closed.");
                    }
                    // Unfortunately, the ZooKeeper states are not well-documented. This represents the
                    // best guess with an incomplete understanding.
                    LOG.info("Connection flagged as invalid with state: {}", this.zooKeeper.getState());
                    switch (this.zooKeeper.getState()) {
                        case CONNECTING:
                            LOG.info("Waiting until connected...");
                            this.token.wait(3000);
                            break;
                        case AUTH_FAILED:
                            // No point in retrying
                            throw new ZkException("Authentication to the ZooKeeper Ensemble failed.");
                        case CLOSED:
                        case NOT_CONNECTED:
                            // These states indicate session expiration among
                            // other things.
                            try {
                                LOG.info("Attempting to reconnect...");
                                this.connect();
                            } catch (final IOException | KeeperException e) {
                                LOG.error("Unable to connect to the ZooKeeper Ensemble.", e);
                            }
                        case CONNECTED:
                        case ASSOCIATING:
                        case CONNECTEDREADONLY:
                            // This check ensures that connections made
                            // after the start of this synchronization block
                            // but before the call to zooKeeper#getState()
                            // are detected. Not sure about ASSOCIATING.
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

    /**
     * Updates the current {@link ZooKeeper} Ensemble configuration. Updates to the configuration may trigger a
     * reconnect to balance the load across the new Ensemble.
     * @see ZooKeeper#updateServerList(String)
     * @throws KeeperException if a problem occurs getting the current ZooKeeper Ensemble configuration.
     * @throws InterruptedException if the current thread is interrupted while waiting.
     * @throws IOException if a problem occurs while attempting to connect to the ZooKeeper Ensemble.
     */
    private void updateConfig() throws KeeperException, InterruptedException, IOException {
        final CharSequence newConnectString = new ConnectString(
            this.get().getConfig(
                this,
                this.get().exists(
                    ZooDefs.CONFIG_NODE,
                    false
                )
            )
        );
        if (newConnectString.length() > 0) {
            synchronized (this.token) {
                this.connectString = newConnectString;
                LOG.info("Setting the connectString to {}", this.connectString);
                this.get().updateServerList(this.connectString.toString());
                int tries = 0;
                do {
                    ++tries;
                    this.token.wait(5000);
                } while (!this.isValid && tries < this.retries);
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
        // Synchronizing on the token to ensure
        // this method plays nice with the above
        synchronized (this.token) {
            this.zooKeeper.close();
            this.isValid = false;
            this.isClosed = true;
        }
    }
}
