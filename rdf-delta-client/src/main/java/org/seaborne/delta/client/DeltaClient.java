/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.seaborne.delta.client;

import java.util.Map;
import java.util.Objects ;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.sparql.core.DatasetGraph ;
import org.seaborne.delta.DataSourceDescription ;
import org.seaborne.delta.DeltaBadRequestException ;
import org.seaborne.delta.DeltaConfigException ;
import org.seaborne.delta.Id ;
import org.seaborne.delta.link.DeltaLink ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

/**
 * {@code DeltaClient} is the application interface to managed local state (help in
 * the @link Zone} and connection over a {@link DeltaLink} to the patch log server (local
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
 * {@link #connect(Id, TxnSyncPolicy) connect}. This sets the {@link TxnSyncPolicy} for the
 * connection.
 * <p>The full lifecycle is
 * <pre>
 *     DeltaClient dClient = ...
 *     Id dsRef = dClient.newDataSource(NAME, URI);
 *     dClient.attach(dsRef, LocalStorageType.TDB);
 *     dClient.connect(dsRef, TxnSyncPolicy.TXN_RW);
 *     for(DeltaConnection dConn = dClient.get(dsRef) ) {
 *         Dataset ds = dConn.getDataset();
 *         Txn.executeWrite(ds, ()->{
 *             .. transaction ..
 *         });
 *         Txn.executeRead(ds, ()->{
 *             .. transaction ..
 *         });
 *     }
 * </pre>
 * To restart:
 * <pre>    
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
 * <li> {@link #newDataSource(String, String, LocalStorageType, TxnSyncPolicy)}: create-attach-connect
 * <li> {@link #register(Id, LocalStorageType, TxnSyncPolicy)}: attach(Id)-connect
 * <li> {@link #register(String, LocalStorageType, TxnSyncPolicy)}: attach(Name)-connect
 * </ul>
 * {@link #removeDataSource} ensures local state clean-up is done.
 */
public class DeltaClient {
    private static Logger LOG = LoggerFactory.getLogger(DeltaClient.class);
    
    /** 
     * Create a {@code DeltaClient} which consists of a {@link Zone}, client-side recorded state
     * of the datasets being managed, and a {@link DeltaLink} connection to the patch log server.
     */
    public static DeltaClient create(Zone zone, DeltaLink dLink) {
        Objects.requireNonNull(zone);
        Objects.requireNonNull(dLink);
        return new DeltaClient(zone, dLink); 
    }
    
    private final Zone zone ;
    private final DeltaLink dLink ;
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

//    //No Cache. get() also needs a fixup.
//    private void removeCache(Id id) { }
//    private void putCache(Id id, DeltaConnection dConn) { }
//    private DeltaConnection getCache(Id id) { return null; }

    private static TxnSyncPolicy applyDefault(TxnSyncPolicy syncPolicy) {
        return syncPolicy == null ? TxnSyncPolicy.NONE : syncPolicy;
    }
    
    private DeltaClient(Zone zone, DeltaLink dLink) {
        this.zone = zone;
        this.dLink = dLink;
    }
    
    /** Create a new data source.
     * This operation does not register the new {@code DataSource} to this {@code DeltaClient};
     * call {@link #register(Id, LocalStorageType, TxnSyncPolicy)}.
     */
    public Id newDataSource(String name, String uri) {
        return dLink.newDataSource(name, uri);
    }
    
    /** Create a new data source, setup with local storage, and connect. 
     * This is a convenience operation equivalent to:
     * <pre>
     *   Id dsRef = dLink.newDataSource(name, uri);
     *   register(dsRef, storageType, syncPolicy);
     *   return dsRef;
     * </pre>
     * The choice of {@code storageType} is permanent for the client-side cache.
     * The choice of {@code syncPolicy} applies only to this registration.
     * When restarting call {@link #connect} to 
     */
    public Id newDataSource(String name, String uri, LocalStorageType storageType, TxnSyncPolicy syncPolicy) {
        Id dsRef = dLink.newDataSource(name, uri);
        attach(dsRef, storageType);
        connect(dsRef, syncPolicy);
        return dsRef;
    }
    
    /** Setup local state management. */
    public void attach(String name, LocalStorageType storageType) {
        Objects.requireNonNull(name);
        DataSourceDescription dsd = dLink.getDataSourceDescriptionByName(name);
        if ( dsd == null )
            throw new DeltaBadRequestException("Can't attach: no such link data source : "+name);
        setupState$(dsd, storageType);
    }
    
    /** Setup local state management. */
    public void attach(Id datasourceId, LocalStorageType storageType) {
        Objects.requireNonNull(datasourceId);
        DataSourceDescription dsd = dLink.getDataSourceDescription(datasourceId);
        if ( dsd == null )
            throw new DeltaBadRequestException("Can't attach: no such link data source : "+datasourceId);
        setupState$(dsd, storageType);
    }
    
    private void setupState$(DataSourceDescription dsd, LocalStorageType storageType) {
        Id datasourceId = dsd.getId();
        if ( zone.exists(datasourceId) ) {
            DataState dataState = zone.get(datasourceId);
            throw new DeltaConfigException("Local data source management already exists: "+dataState.getDatasourceName());
        }
        DataState dataState = zone.create(datasourceId, dsd.getName(), dsd.getUri(), storageType);
    }

    /** Create a local zone entry and setup to track the existing remote datasource.
     *  This is a combination of {@link #attach(String, LocalStorageType)} and {@link #connect(Id, TxnSyncPolicy)}.
     * @param name
     * @param storageType
     * @param syncPolicy
     * @return Id
     */
    public Id register(String name, LocalStorageType storageType, TxnSyncPolicy syncPolicy) {
        attach(name, storageType);
        Id dsRef = nameToId(name);
        connect(dsRef, syncPolicy);
        return dsRef;
    }

    /** Create a local zone entry and setup to track the existing remote datasource.
     * 
     * @param datasourceId
     * @param storageType
     * @param syncPolicy
     */
    public void register(Id datasourceId, LocalStorageType storageType, TxnSyncPolicy syncPolicy) {
        attach(datasourceId, storageType);
        connect(datasourceId, syncPolicy);
    }

    /** 
     * Connect to an existing {@code DataSource} with existing local state.
     */
    public void connect(Id datasourceId, TxnSyncPolicy syncPolicy) {
        syncPolicy = applyDefault(syncPolicy);
        if ( ! zone.exists(datasourceId) )
            throw new DeltaConfigException("Data source '"+datasourceId.toString()+"' not found for this DeltaClient");
        DataState dataState = zone.connect(datasourceId);
        DatasetGraph dsg = zone.getDataset(dataState);
        DeltaConnection dConn = DeltaConnection.create(dataState, dsg, dLink, syncPolicy);
        putCache(datasourceId, dConn);
    }
    
    /** 
     * Connect to an existing {@code DataSource} with existing local state.
     */
    public Id connect(String name, TxnSyncPolicy syncPolicy) {
        Id dsRef = nameToId(name);
        connect(dsRef, syncPolicy);
        return dsRef;
    }

    /**
     * Attach to an existing {@code DataSource} with a fresh
     * {@link DatasetGraph} as local state. The caller undertakes to only access
     * the {@code DatasetGraph} through a {@link DeltaConnection} obtained from
     * this {@code DeltaClient}.
     * <p>
     * This is a specialised operation - using a managed dataset (see
     * {@link #attach(String, LocalStorageType)}) is preferred.
     * <p>
     * The {@code DatasetGraph} is assumed to empty and is brought up-to-date.
     * The client must be registered with the {@code DeltaLink}.
     * <p>
     * {@link #connect(Id, TxnSyncPolicy)} must be called later to use the dataset.
     */
    
    public Id attachExternal(String name, DatasetGraph dsg) {
        DataSourceDescription dsd = dLink.getDataSourceDescriptionByName(name);
        if ( dsd == null )
            throw new DeltaBadRequestException("Can't attach: no such link data source : "+name);
        setupExternal(dsd, dsg);
        return dsd.getId();
    }

    /**
     * Attach to an existing {@code DataSource} with a fresh
     * {@link DatasetGraph} as local state. The caller undertakes to only access
     * the {@code DatasetGraph} through a {@link DeltaConnection} obtained from
     * this {@code DeltaClient}.
     * <p>
     * The {@code DatasetGraph} is assumed to empty and is brought up-to-date.
     * The client must be registered with the {@link DeltaLink}.
     * <p>
     * This is a specialised operation - using a managed dataset (see
     * {@link #register(Id, LocalStorageType, TxnSyncPolicy)}) is preferred.
     * <p>
     * {@link #connect(Id, TxnSyncPolicy)} must be called later to use the dataset.
     */
    public void attachExternal(Id datasourceId, DatasetGraph dsg) {
        Objects.requireNonNull(datasourceId);
        DataSourceDescription dsd = dLink.getDataSourceDescription(datasourceId);
        if ( dsd == null )  
            throw new DeltaBadRequestException("Can't attach: no such link data source : "+datasourceId);
        setupExternal(dsd, dsg);
    }
    
    private void setupExternal(DataSourceDescription dsd, DatasetGraph dsg) {
        //DataSourceDescription dsd = dLink.getDataSourceDescription(datasourceId);
        Id datasourceId = dsd.getId();
        if ( zone.exists(datasourceId) ) {
            DataState dataState = zone.get(datasourceId);
            throw new DeltaConfigException("Can't attach: data source already exists locally: "+dataState.getDatasourceName());
        }
        DataState dataState = zone.create(datasourceId, dsd.getName(), dsd.getUri(), LocalStorageType.EXTERNAL);
    }
    
    /** Get the {@link Id} for a given short name for the {@link DeltaLink} for this pool. 
     * Rerturns null if there is no attached local statement management.
     */ 
    public Id nameToId(String name) {
        return zone.getIdForName(name);
//        return dLink.listDescriptions().stream()
//            .filter(dsd->name.equals(dsd.getName()))
//            .map(dsd->dsd.getId())
//            .findAny()
//            .orElse(null);
    }
    
    public void connect(Id datasourceId, DatasetGraph dsg, TxnSyncPolicy syncPolicy) {
        Objects.requireNonNull(datasourceId);
        DeltaConnection dConn = get(datasourceId);
        if ( dConn != null )
            return;
        DataState dataState = zone.get(datasourceId);
        dConn = DeltaConnection.create(dataState, dsg, dLink, syncPolicy);
        putCache(datasourceId, dConn);
    }
    
    /**
     * Get a {@link DeltaConnection}. 
     * It is not automatically up-to-date - that depends on the {@link TxnSyncPolicy}
     * set when the DataSource was registered with this client.
     * Either it is done on transaction boundaries, or the application can call
     * {@link DeltaConnection#sync()}. The caller must close this object -
     * {@code try-with-resources} is supported. Returns null if the
     * {@code dsRef} does not identify a data source that has been registered with
     * a registation operation.
     * 
     * @param dsRef
     * @return DeltaConnection or null
     */
    public DeltaConnection get(Id dsRef) {
        DeltaConnection dConn = getCache(dsRef);
        if ( dConn == null )
            return null; // throw ?
        dConn.sync();
        return dConn;
    }
    
    /**
     * Get a {@link DeltaConnection}. 
     * It is not automatically up-to-date - that depends on the {@link TxnSyncPolicy}
     * set when the DataSource was registered with this client.
     * Either it is done on transaction boundaries, or the application can call
     * {@link DeltaConnection#sync()}. The caller must close this object -
     * {@code try-with-resources} is supported. Returns null if the
     * {@code dsRef} does not identify a data source that has been registered with
     * a registation operation.
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

//    public void printState() {
//        PrintStream out = System.out;
//        out.println("DeltaClient:");
//        connections.forEach((id, dc)->{
//            DataState ds = zone.get(id);
//            out.printf("  Id = %s State=%s\n", id, ds);
//        });
//        
//    }

    public Zone getZone() {
        return zone ;
    }

    public DeltaLink getLink() {
        return dLink ;
    }
}
