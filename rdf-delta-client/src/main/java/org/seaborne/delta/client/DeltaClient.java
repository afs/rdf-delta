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

import java.io.PrintStream ;
import java.util.Map ;
import java.util.Objects ;
import java.util.concurrent.ConcurrentHashMap ;

import org.apache.jena.sparql.core.DatasetGraph ;
import org.seaborne.delta.DataSourceDescription ;
import org.seaborne.delta.DeltaBadRequestException ;
import org.seaborne.delta.DeltaConfigException ;
import org.seaborne.delta.Id ;
import org.seaborne.delta.link.DeltaLink ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

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

    private DeltaClient(Zone zone, DeltaLink dLink) {
        this.zone = zone;
        this.dLink = dLink;
    }
    
    /** Create a new daat source and regster it with this {@code DeltaClient}. 
     * To just create a remote data source, use {@link DeltaLink#newDataSource}.
     * @param name
     * @param uri
     * @param storage
     * @return Id
     */
    public Id newDataSource(String name, String uri, LocalStorageType storage) {
        Id dsRef = dLink.newDataSource(name, uri);
        attach(dsRef, storage);
        return dsRef;
    }
    
    /** Create a local zone entry and setup to track the existing remote datasource */
    public Id attach(String name, LocalStorageType storageType) {
        Id dsRef = nameToId(name);
        attach(dsRef, storageType);
        return dsRef;
    }

    /** Create a local zone entry and setup to track the existing remote datasource */
    public void attach(Id datasourceId, LocalStorageType storageType) {
        Objects.requireNonNull(datasourceId);
        if (  zone.exists(datasourceId) ) {
            DataState dataState = zone.get(datasourceId);
            throw new DeltaConfigException("Can't attach: data source already exists locally: "+dataState.getDatasourceName());
        }
        DeltaConnection dConn = get(datasourceId);
        if ( dConn != null )
            return;
        DataSourceDescription dsd = dLink.getDataSourceDescription(datasourceId);
        if ( dsd == null )
            throw new DeltaBadRequestException("Can't attach: no such link data source : "+datasourceId);
        DataState dataState = zone.create(datasourceId, dsd.getName(), dsd.getUri(), storageType);
        dConn = DeltaConnection.connect(dataState, zone.getDataset(dataState), dLink);
        connections.put(datasourceId, dConn);
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
     * See {@link DeltaConnection#connect} for connecting an existing local
     * dataset. The client must be registered with the {@code DeltaLink}.
     */
    
    public Id attachExternal(String name, DatasetGraph dsg) {
        Id dsRef = nameToId(name);
        attachExternal(dsRef, dsg);
        return dsRef;
    }

    /**
     * Attach to an existing {@code DataSource} with a fresh
     * {@link DatasetGraph} as local state. The caller undertakes to only access
     * the {@code DatasetGraph} through a {@link DeltaConnection} obtained from
     * this {@code DeltaClient}.
     * <p>
     * This is a specialised operation - using a managed dataset (see
     * {@link #attach(Id, LocalStorageType)}) is preferred.
     * <p>
     * The {@code DatasetGraph} is assumed to empty and is brought up-to-date.
     * See {@link DeltaConnection#connect} for connecting an existing local
     * dataset. The client must be registered with the {@link DeltaLink}.
     */
    
    public void attachExternal(Id datasourceId, DatasetGraph dsg) {
        Objects.requireNonNull(datasourceId);
        if ( get(datasourceId) != null )
            // XXX Idempotent. Is this a good idea?
            return;
        if ( zone.exists(datasourceId) ) {
            DataState dataState = zone.get(datasourceId);
            throw new DeltaConfigException("Can't attach: data source already exists locally: "+dataState.getDatasourceName());
        }
        
        DataSourceDescription dsd = dLink.getDataSourceDescription(datasourceId);
        DataState dataState = zone.create(datasourceId, dsd.getName(), dsd.getUri(), LocalStorageType.EXTERNAL);
        
        DeltaConnection dConn = DeltaConnection.connect(dataState, dsg, dLink);
        dConn.sync();
        connections.put(datasourceId, dConn);
    }
    
    /** 
     * Connect to an existing {@code DataSource} with existing local state.
     */
    public void connect(Id datasourceId) {
        if ( ! zone.exists(datasourceId) )
            throw new DeltaConfigException("Data source '"+datasourceId.toString()+"' not found for this DeltaClient");
        DataState dataState = zone.connect(datasourceId);
        DatasetGraph dsg = zone.getDataset(dataState);
        DeltaConnection dConn = DeltaConnection.connect(dataState, dsg, dLink);
        connections.put(datasourceId, dConn);
    }
    
    /** 
     * Connect to an existing {@code DataSource} with existing local state.
     */
    public Id connect(String name) {
        Id dsRef = nameToId(name);
        connect(dsRef);
        return dsRef;
    }

    /** Get the {@link Id} for a given short name for the {@link DeltaLink} for this pool. */ 
    public Id nameToId(String name) {
        return zone.getIdForName(name);
//        return dLink.listDescriptions().stream()
//            .filter(dsd->name.equals(dsd.getName()))
//            .map(dsd->dsd.getId())
//            .findAny()
//            .orElse(null);
    }
    
    // XXX Does not need to sync.
    public void connect(Id datasourceId, DatasetGraph dsg) {
        Objects.requireNonNull(datasourceId);
        DeltaConnection dConn = get(datasourceId);
        if ( dConn != null )
            return;
        DataState dataState = zone.get(datasourceId);
        dConn = DeltaConnection.connect(dataState, dsg, dLink);
        connections.put(datasourceId, dConn);
    }
    
    /**
     * Get a {@link DeltaConnection}. It is not automatically up-to-date - see
     * {@link DeltaConnection#sync()}. The caller must close this object -
     * {@code try-with-resources} is supported. Returns null if the
     * {@code dsRef} dioes not identify a data source that has been set up with
     * {@link #attach} or {@link #connect}.
     * 
     * @param dsRef
     * @return DeltaConnection or null
     */
    public DeltaConnection get(Id dsRef) {
        DeltaConnection dConn = connections.get(dsRef);
        if ( dConn == null )
            return null; // throw ?
        return dConn;
    }
    
    /**
     * Get a {@link DeltaConnection}. It is not automatically up-to-date - see
     * {@link DeltaConnection#sync()}. The caller must close this object -
     * {@code try-with-resources} is supported. Returns null if the
     * {@code dsRef} dioes not identify a data source that has been set up with
     * {@link #attach} or {@link #connect}.
     * 
     * @param name
     * @return DeltaConnection or null
     */
    public DeltaConnection get(String name) {
        return get(nameToId(name));
    }

    // XXX 
    public void removeDataSource(Id datasourceId) {
        LOG.warn("CHECK removeDataSource");
        checkDeltaClient();
        connections.remove(datasourceId);
        zone.delete(datasourceId);
        dLink.removeDataSource(datasourceId);
    }

    private void checkDeltaClient() {}

    public void printState() {
        PrintStream out = System.out;
        out.println("DeltaClient:");
        connections.forEach((id, dc)->{
            DataState ds = zone.get(id);
            out.printf("  Id = %s State=%s\n", id, ds);
        });
        
    }

    public Zone getZone() {
        return zone ;
    }

    public DeltaLink getLink() {
        return dLink ;
    }
}
