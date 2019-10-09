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

package org.seaborne.delta.client;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.sparql.core.DatasetGraph;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.Id;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code DeltaClient} is the application interface to managed local state (held in
 * the {@link Zone}) and connection over a {@link DeltaLink} to the patch log server (local
 * or remote).
 * <p>
 * Lifecycle of a data source involves:
 * <p>
 * A data source must be created in the patch log server - this happens once for all
 * clients in {@link #newDataSource(String, String) newDataSource}.
 * <p>
 * At each client, the local state management and caching must be set up - this happens
 * once in each client in {@link #attach(Id, LocalStorageType) attach}. This sets the
 * {@link LocalStorageType} for the local state management.
 * <p>
 * A managed connection (a {@link DeltaConnection}) is used for operations on the data
 * source - this is created in each client, every time the application runs in
 * {@link #connect(Id, SyncPolicy) connect}. This sets the {@link SyncPolicy} for the
 * connection.
 * <p>The full lifecycle from creating the data source is:
 * <pre>
 *     DeltaClient dClient = DeltaClient.create(zone, deltaLink);
 *     Id dsRef = dClient.newDataSource(NAME, URI);
 *     dClient.attach(dsRef, LocalStorageType.TDB);
 *     dClient.connect(dsRef, TxnSyncPolicy.TXN_RW);
 *     for(DeltaConnection dConn = dClient.get(dsRef) ) {
 *         Dataset ds = dConn.getDataset();
 *         Txn.executeWrite(ds, ()-&gt;{
 *             .. transaction ..
 *         });
 *         Txn.executeRead(ds, ()-&gt;{
 *             .. transaction ..
 *         });
 *     }
 * </pre>
 * and each time a JVM restarts it needs to connect each data source:
 * <pre>
 *     DeltaClient dClient = DeltaClient.create(zone, deltaLink);
 *     dClient.connect(dsRef, TxnSyncPolicy.TXN_RW);
 *     ...
 * </pre>
 * To remove:
 * <pre>
 *     // To remove local management (undo "attach"):
 *     dClient.release(dsRef);
 *
 *     // To delete everywhere.
 *     dClient.removeDataSource(dsRef);
 * </pre>
 * <p>
 * Convenience combined operations are provided:
 * <ul>
 * <li> {@link #createDataSource(String, String, LocalStorageType, SyncPolicy)}: create-attach-connect
 * <li> {@link #register(Id, LocalStorageType, SyncPolicy)}: attach(Id)-connect
 * <li> {@link #register(String, LocalStorageType, SyncPolicy)}: attach(Name)-connect
 * </ul>
 * {@link #removeDataSource} ensures local state clean-up is done.
 * <p>
 * {@code LocalStorageType.NONE} indicates that there is no RDF dataset to be managed - the
 * patch log version tracking is stills available.
 * </p>
 * <p>
 * {@code LocalStorageType.EXTERNAL} indicates the dataset is provided by the application.
 * Specific operation {@link #attachExternal}, and the attach-connect combined operation
 * of {@link #registerExternal}, enable the application to pass in a dataset. The
 * application is responsible for the initial state of the data. If the application is
 * using RDF Delta to synchronize this data, it must start in the same initial state on
 * each machine, including same blank node internal identifiers. This can be achieved by
 * copying a single source to all machines.
 * </p>
 */
public class DeltaClient {
    private static Logger LOG = LoggerFactory.getLogger(DeltaClient.class);

    /**
     * Create a {@code DeltaClient} which consists of a {@link Zone}, client-side recorded
     * state of the dataset being managed, and a {@link DeltaLink} connection to the patch
     * log server.
     */
    public static DeltaClient create(Zone zone, DeltaLink dLink) {
        Objects.requireNonNull(zone);
        Objects.requireNonNull(dLink);
        return new DeltaClient(zone, dLink);
    }

    private final Zone               zone;
    private final DeltaLink          dLink;
    // For now, non-counting.
    private Map<Id, DeltaConnection> connections = new ConcurrentHashMap<>();

    private void removeCache(Id id) {
        connections.remove(id);
    }

    private void putCache(Id id, DeltaConnection dConn) {
        if ( dConn == null )
            connections.remove(id);
        else
            connections.put(id, dConn);
    }

    private DeltaConnection getCache(Id id) {
        return connections.get(id);
    }

// //No Cache. get() also needs a fixup.
// private void removeCache(Id id) { }
// private void putCache(Id id, DeltaConnection dConn) { }
// private DeltaConnection getCache(Id id) { return null; }

    private static SyncPolicy applyDefault(SyncPolicy syncPolicy) {
        return syncPolicy == null ? SyncPolicy.NONE : syncPolicy;
    }

    private DeltaClient(Zone zone, DeltaLink dLink) {
        this.zone = zone;
        this.dLink = dLink;
    }

    /**
     * Create a new data source. This operation does not register the new
     * {@code DataSource} to this {@code DeltaClient}; call
     * {@link #register(Id, LocalStorageType, SyncPolicy)}.
     */
    public Id newDataSource(String name, String uri) {
        return dLink.newDataSource(name, uri);
    }

    /**
     * Create a new data source, setup with local storage, and connect. This is a
     * convenience operation equivalent to:
     *
     * <pre>
     * Id dsRef = dLink.newDataSource(name, uri);
     * register(dsRef, storageType, syncPolicy);
     * return dsRef;
     * </pre>
     *
     * The choice of {@code storageType} is permanent for the client-side cache. The
     * choice of {@code syncPolicy} applies only to this registration. When restarting
     * call {@link #connect} to
     */
    public Id createDataSource(String name, String uri, LocalStorageType storageType, SyncPolicy syncPolicy) {
        Id dsRef = dLink.newDataSource(name, uri);
        register(dsRef, storageType, syncPolicy);
        // Which is:
        // attach(dsRef, storageType);
        // DeltaConnection dConn = connect(dsRef, syncPolicy);
        return dsRef;
    }

    /**
     * Setup local state management.
     * <p>
     * This operation contacts the patch log server.
     */
    public void attach(String name, LocalStorageType storageType) {
        Objects.requireNonNull(name, "name");
        DataSourceDescription dsd = dLink.getDataSourceDescriptionByName(name);
        if ( dsd == null )
            throw new DeltaBadRequestException("Can't attach: no such link data source by name : " + name);
        setupState$(dsd, storageType);
    }

    /**
     * Setup local state management.
     * <p>
     * This operation contacts the patch log server.
     */
    public void attach(Id datasourceId, LocalStorageType storageType) {
        Objects.requireNonNull(datasourceId, "datasourceId");
        DataSourceDescription dsd = dLink.getDataSourceDescription(datasourceId);
        if ( dsd == null )
            throw new DeltaBadRequestException("Can't attach: no such link data source by id : " + datasourceId);
        setupState$(dsd, storageType);
    }

    /**
     * Attach to an existing {@code DataSource} with a fresh {@link DatasetGraph} as local
     * state. The caller undertakes to only access the {@code DatasetGraph} through a
     * {@link DeltaConnection} obtained from this {@code DeltaClient}.
     * <p>
     * This is a specialised operation - using a managed dataset (see
     * {@link #attach(String, LocalStorageType)}) is preferred.
     * <p>
     * The {@code DatasetGraph} is assumed to be empty and is brought up-to-date. The
     * client must be registered with the {@code DeltaLink}.
     * <p>
     * {@link #connect(Id, SyncPolicy)} must be called later to use the dataset.
     */

    public Id attachExternal(String name, DatasetGraph dsg) {
        Objects.requireNonNull(name);
        DataSourceDescription dsd = dLink.getDataSourceDescriptionByName(name);
        if ( dsd == null )
            throw new DeltaBadRequestException("Can't attach: no such link data source : " + name);
        setupExternal(dsd, dsg);
        return dsd.getId();
    }

    /**
     * Attach to an existing {@code DataSource} with a fresh {@link DatasetGraph} as local
     * state. The caller undertakes to only access the {@code DatasetGraph} through a
     * {@link DeltaConnection} obtained from this {@code DeltaClient}.
     * <p>
     * The {@code DatasetGraph} is assumed to be empty and is brought up-to-date. The
     * client must be registered with the {@link DeltaLink}.
     * <p>
     * This is a specialised operation - using a managed dataset (see
     * {@link #register(Id, LocalStorageType, SyncPolicy)}) is preferred.
     * <p>
     * {@link #connect(Id, SyncPolicy)} must be called later to use the dataset.
     */
    public void attachExternal(Id datasourceId, DatasetGraph dsg) {
        Objects.requireNonNull(datasourceId);
        DataSourceDescription dsd = dLink.getDataSourceDescription(datasourceId);
        if ( dsd == null )
            throw new DeltaBadRequestException("Can't attach: no such link data source : " + datasourceId);
        setupExternal(dsd, dsg);
    }

    private void setupState$(DataSourceDescription dsd, LocalStorageType storageType) {
        Id datasourceId = dsd.getId();
        if ( zone.exists(datasourceId) ) {
            DataState dataState = zone.get(datasourceId);
            throw new DeltaConfigException("Local data source management already exists: " + dataState.getDatasourceName());
        }
        DataState dataState = zone.create(datasourceId, dsd.getName(), dsd.getUri(), storageType);
    }

    /**
     * Create a local zone entry and setup to track the existing remote datasource.
     * <p>
     * This is a combination of {@link #attach(String, LocalStorageType)} and
     * {@link #connect(Id, SyncPolicy)}.
     * <p>
     * This operation contacts the patch log server (see {@link #attach}).
     *
     * @param name
     * @param storageType
     * @param syncPolicy
     * @return Id
     */
    public Id register(String name, LocalStorageType storageType, SyncPolicy syncPolicy) {
        attach(name, storageType);
        Id dsRef = nameToId(name);
        connect(dsRef, syncPolicy);
        return dsRef;
    }

    /**
     * Create a local zone entry and setup to track the existing remote datasource. This
     * operation is equivalent to:
     *
     * <pre>
     * attach(datasourceId, storageType);
     * connect(datasourceId, syncPolicy);
     * </pre>
     * <p>
     * This operation contacts the patch log server (see {@link #attach}).
     *
     * @param datasourceId
     * @param storageType
     * @param syncPolicy
     */
    public DeltaConnection register(Id datasourceId, LocalStorageType storageType, SyncPolicy syncPolicy) {
        attach(datasourceId, storageType);
        DeltaConnection dConn = connect(datasourceId, syncPolicy);
        return dConn;
    }

    /**
     * Create a local zone entry and setup to track the existing remote datasource. This
     * operation is equivalent to:
     *
     * <pre>
     * attachExternal(datasourceId, datasetGraph);
     * connect(datasourceId, syncPolicy);
     * </pre>
     * <p>
     * This operation contacts the patch log server (see {@link #attach}).
     *
     * @param datasourceId
     * @param datasetGraph
     * @param syncPolicy
     */
    public DeltaConnection registerExternal(Id datasourceId, DatasetGraph datasetGraph, SyncPolicy syncPolicy) {
        attachExternal(datasourceId, datasetGraph);
        DeltaConnection dConn = connect(datasourceId, syncPolicy);
        return dConn;
    }

    /**
     * Connect to an existing {@code DataSource} with existing local state. This operation
     * does not fail if it can not contact the patch log server.
     *
     * @param datasourceId
     * @param syncPolicy
     */
    public DeltaConnection connect(Id datasourceId, SyncPolicy syncPolicy) {
        syncPolicy = applyDefault(syncPolicy);
        if ( !zone.exists(datasourceId) )
            throw new DeltaConfigException("Data source '" + datasourceId.toString() + "' not found for this DeltaClient");
        DataState dataState = zone.connect(datasourceId);
        DatasetGraph dsg = zone.getDataset(dataState);
        DeltaConnection dConn = DeltaConnection.create(dataState, dsg, dLink, syncPolicy);
        putCache(datasourceId, dConn);
        return dConn;
    }

    /**
     * Connect to an existing {@code DataSource} with existing local state. This operation
     * does not fail if it can not contact the patch log server.
     *
     * @param name
     * @param syncPolicy
     */
    public Id connect(String name, SyncPolicy syncPolicy) {
        Id dsRef = nameToId(name);
        connect(dsRef, syncPolicy);
        return dsRef;
    }

    /**
     * Connect to an existing {@code DataSource} providing the local state in an external database.
     * The caller is responsible for this external state being correct for the version recorded in the zone.
     * This operation does not fail if it can not contact the patch log server.
     *
     * @param datasourceId
     * @param dsg
     * @param syncPolicy
     */
    public void connectExternal(Id datasourceId, DatasetGraph dsg, SyncPolicy syncPolicy) {
        Objects.requireNonNull(datasourceId);
        DeltaConnection dConn = get(datasourceId);
        if ( dConn != null )
            return;
        zone.externalStorage(datasourceId, dsg);
        DataState dataState = zone.get(datasourceId);
        dConn = DeltaConnection.create(dataState, dsg, dLink, syncPolicy);
        putCache(datasourceId, dConn);
    }

    /** Supply a dataset for matching to an attached external data source */
    private void setupExternal(DataSourceDescription dsd, DatasetGraph dsg) {
        Id datasourceId = dsd.getId();
        if ( zone.exists(datasourceId) ) {
            DataState dataState = zone.get(datasourceId);
            throw new DeltaConfigException("Can't attach: data source already exists locally: " + dataState.getDatasourceName());
        }
        DataState dataState = zone.create(datasourceId, dsd.getName(), dsd.getUri(), LocalStorageType.EXTERNAL);
        externalStorage(datasourceId, dsg);
    }

    /** Supply a dataset for matching to an attached external data source */
    private void externalStorage(Id datasourceId, DatasetGraph dsg) {
        if ( !zone.exists(datasourceId) )
            throw new DeltaConfigException("Can't add external storage: data source not attached to this zone: " + datasourceId);
        DataState dataState = zone.get(datasourceId);
        if ( !LocalStorageType.EXTERNAL.equals(dataState.getStorageType()) ) {
            throw new DeltaConfigException("Can't add external storage: data source is not 'external': " + datasourceId);
        }
        zone.externalStorage(datasourceId, dsg);
    }

    /**
     * Get the {@link Id} for a given short name for the {@link DeltaLink} for this pool.
     * Returns null if there is no attached local statement management.
     */
    public Id nameToId(String name) {
        return zone.getIdForName(name);
    }

    /**
     * Test whether a data source Id is available locally, that is, there is data state in
     * the zone.
     */
    public boolean existsLocal(Id datasourceId) {
        return zone.exists(datasourceId);
    }

    /**
     * Test whether a data source Id is available remotely, that is, there is log in the
     * server.
     */
    public boolean existsRemote(Id datasourceId) {
        return dLink.getDataSourceDescription(datasourceId) != null;
    }

    /**
     * Get a {@link DeltaConnection}. It is not automatically up-to-date - that depends on
     * the {@link SyncPolicy} set when the DataSource was registered with this client.
     * Either it is done on transaction boundaries, or the application can call
     * {@link DeltaConnection#sync()}. The caller must close this object -
     * {@code try-with-resources} is supported. Returns null if the {@code dsRef} does not
     * identify a data source that has been registered with a registation operation.
     *
     * @param dsRef
     * @return DeltaConnection or null
     */
    public DeltaConnection get(Id dsRef) {
        DeltaConnection dConn = getCache(dsRef);
        if ( dConn == null )
            return null; // throw ?
        dConn.trySyncIfAuto();
        return dConn;
    }

    /**
     * Get the {@link DeltaConnection} but only return from the local cache and do not try
     * to contact the patch log server and do not sync according to policy.
     *
     * @param dsRef
     * @return DeltaConnection or null
     */
    public DeltaConnection getLocal(Id dsRef) {
        DeltaConnection dConn = getCache(dsRef);
        return dConn;
    }

    /**
     * Get a {@link DeltaConnection}. It is not automatically up-to-date - that depends on
     * the {@link SyncPolicy} set when the DataSource was registered with this client.
     * Either it is done on transaction boundaries, or the application can call
     * {@link DeltaConnection#sync()}. The caller must close this object -
     * {@code try-with-resources} is supported. Returns null if the {@code dsRef} does not
     * identify a data source that has been registered with a registation operation.
     *
     * @param name
     * @return DeltaConnection or null
     */
    public DeltaConnection get(String name) {
        return get(nameToId(name));
    }

    public void release(Id datasourceId) {
        checkDeltaClient();
        releaseLocal(datasourceId);
    }

    /** Remove client side - cache and zone. */
    private void releaseLocal(Id datasourceId) {
        // Remove from local setup first.
        removeCache(datasourceId);
        if ( zone.exists(datasourceId) )
            zone.delete(datasourceId);
    }

    public void removeDataSource(Id datasourceId) {
        checkDeltaClient();
        // Remove from local setup first.
        releaseLocal(datasourceId);
        // Then remove remotely.
        dLink.removeDataSource(datasourceId);
    }

    private void checkDeltaClient() {}

// public void printState() {
// PrintStream out = System.out;
// out.println("DeltaClient:");
// connections.forEach((id, dc)->{
// DataState ds = zone.get(id);
// out.printf(" Id = %s State=%s\n", id, ds);
// });
//
// }

    public Zone getZone() {
        return zone;
    }

    public DeltaLink getLink() {
        return dLink;
    }
}
