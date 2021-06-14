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
import java.util.EnumSet;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * If {@link #close()} is called, calls to {@link #zooKeeper()} will yield a {@link ZkException} that should crash the
 * system. Only call {@link #close()} when you are done with this {@link ValidatedZooKeeperProvider}.
 * </p>
 */
public final class ValidatedZooKeeperProvider implements ZooKeeperProvider, Watcher, AutoCloseable {
    /**
     * {@link Logger}.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ValidatedZooKeeperProvider.class);

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
    private final CyclicBarrier connectedSignal;

    /**
     * The set of states considered valid. Used to check the current connection state.
     */
    private final EnumSet<ZooKeeper.States> validStates;

    /**
     * Indicates whether {@link #close()} has been called.
     */
    private final AtomicBoolean isClosed;

    /**
     * Instantiates a {@link ValidatedZooKeeperProvider} with the given connectString specifying a default retry limit of 5.
     * @param connectString A comma-separated list of host:port pairs pointing to ZooKeeper Ensemble members.
     * @throws IOException if a problem occurs while attempting to connect to the ZooKeeper Ensemble.
     * @throws KeeperException if a problem occurs getting the current ZooKeeper Ensemble configuration.
     * @throws InterruptedException if the current thread is interrupted while waiting.
     */
    public ValidatedZooKeeperProvider(final CharSequence connectString) throws IOException, KeeperException, InterruptedException {
        this(connectString, 5);
    }

    /**
     * Instantiates a {@link ValidatedZooKeeperProvider} with the given connectString and the given retry limit.
     * @param connectString A comma-separated list of host:port pairs pointing to ZooKeeper Ensemble members.
     * @param retries The number of times to try to connect to a ZooKeeper Ensemble before giving up.
     * @throws IOException if a problem occurs while attempting to connect to the ZooKeeper Ensemble.
     * @throws KeeperException if a problem occurs getting the current ZooKeeper Ensemble configuration.
     * @throws InterruptedException if the current thread is interrupted while waiting.
     */
    public ValidatedZooKeeperProvider(final CharSequence connectString, final int retries) throws IOException, KeeperException, InterruptedException {
        this.connectString = connectString;
        this.retries = retries;
        this.connectedSignal = new CyclicBarrier(2);
        this.validStates = EnumSet.of(ZooKeeper.States.CONNECTED, ZooKeeper.States.CONNECTEDREADONLY, ZooKeeper.States.ASSOCIATING);
        this.isClosed = new AtomicBoolean(false);
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
                switch (watchedEvent.getState()) {
                    case SyncConnected:
                    case SaslAuthenticated:
                    case ConnectedReadOnly:
                        if (this.connectedSignal.getNumberWaiting() == 1) {
                            try {
                                try {
                                    this.connectedSignal.await();
                                } catch (final BrokenBarrierException e) {
                                    LOG.error("Lost the race.", e);
                                }
                            } catch (final InterruptedException e) {
                                LOG.error("The unthinkable has happened.", e);
                                throw new IllegalStateException(e);
                            }
                        }
                }
            }
        );
        this.updateConfig();
    }

    @Override
    public ZooKeeper zooKeeper() {
        try {
            long tries = 0;
            while (!this.validStates.contains(this.zooKeeper.getState())) {
                if (this.isClosed.get()) {
                    throw new ZkException("ValidZooKeeperSupplier instance has been closed.");
                }
                // Unfortunately, the ZooKeeper states are not well-documented. This represents the
                // best guess with an incomplete understanding.
                LOG.info("Connection flagged as invalid with state: {}", this.zooKeeper.getState());
                switch (this.zooKeeper.getState()) {
                    case CONNECTING:
                        LOG.info("Waiting until connected...");
                        try {
                            this.connectedSignal.await(3, TimeUnit.SECONDS);
                        } catch (final TimeoutException ignored) {
                            this.connectedSignal.reset();
                        }
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
            return this.zooKeeper;
        } catch (final InterruptedException | BrokenBarrierException e) {
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
            this.zooKeeper().getConfig(
                this,
                this.zooKeeper().exists(
                    ZooDefs.CONFIG_NODE,
                    false
                )
            )
        );
        if (newConnectString.length() > 0) {
            this.connectString = newConnectString;
            LOG.info("Setting the connectString to {}", this.connectString);
            this.zooKeeper().updateServerList(this.connectString.toString());
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
        this.isClosed.set(true);
    }
}
