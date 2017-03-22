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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.DatasetGraph;
import org.seaborne.delta.*;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.changes.RDFChangesApply ;
import org.seaborne.patch.changes.RDFChangesCollector;
import org.seaborne.patch.system.DatasetGraphChanges;
import org.slf4j.Logger;

/** Provides an interface to a specific dataset over the general {@link DeltaLink} API. 
 * This is the client API, c.f. JDBC connection
 */ 
public class DeltaConnection implements AutoCloseable {
    
    public enum Backing { TDB, FILE }
    
    // Need local version tracking?
    //   yes - for bring DSG up to date.
    
    // Or via DeltaLink?
    
    
    private static Logger LOG = Delta.DELTA_LOG;
    
    // The version of the remote copy.
    
    private final DeltaLink dLink ;
    
    private final AtomicInteger remoteVersion = new AtomicInteger(0);

    private final DatasetGraph base;
    private final DatasetGraphChanges managed;
    
    private final RDFChanges target;
    private final Id datasourceId;
    private final DataState state;

    /**
     * Create and connect to a new {@code DataSource}. 
     * The caller must be registered with the {@code DelatLink}.
     */
    public static DeltaConnection create(Zone zone, Id clientId, String datasourceName, String uri, DatasetGraph dsg, DeltaLink dLink) {
        Objects.requireNonNull(datasourceName, "Null datasource name");
        Objects.requireNonNull(dLink, "Null link");
        ensureRegistered(dLink, clientId);
        
        Id datasourceId = dLink.newDataSource(datasourceName, uri);
        DataState dataState = zone.create(datasourceName, datasourceId, Backing.TDB);
        DeltaConnection client = DeltaConnection.connect(dataState, datasourceId, dsg, dLink);
        return client;
    }

    /** 
     * Connect to an existing {@code DataSource}.
     * Must be registered with the {@code DeltaLink}.
     */  
    public static DeltaConnection connect(Zone zone, Id clientId, Id datasourceId, DatasetGraph dsg, DeltaLink dLink) {
        Objects.requireNonNull(datasourceId, "Null data source Id");
        Objects.requireNonNull(dLink, "Null link");
        ensureRegistered(dLink, clientId);
        
        // DataSourceDescription desc = dLink.getDataSourceDescription(datasourceId);
        
        if ( ! zone.exists(datasourceId) ) {
            // No local - is there a remote?
            DataSourceDescription dsd = dLink.getDataSourceDescription(datasourceId);
            if ( dsd == null ) {
                // Does not exist.
                throw new DeltaBadRequestException("No such datasorce: "+datasourceId);
                // Autocreate?
                //DeltaConnection dConn = create(clientId, dsd.name, dsd.uri, dsg, dLink);
            }
            DataState dataState = zone.create(dsd.name, datasourceId, Backing.TDB);
            DeltaConnection dConn = DeltaConnection.connect(dataState, datasourceId, dsg, dLink);
            return dConn;
        }
        // Disk refresh.
        zone.refresh(datasourceId);
        DataState dataState = zone.attach(datasourceId);
        if ( ! Objects.equals(datasourceId, dataState.getDataSourceId()) )
            throw new DeltaException("State ds "+dataState.getDataSourceId()+" but app passed "+datasourceId);
        DeltaConnection client = DeltaConnection.connect(dataState, datasourceId, dsg, dLink);
        return client;
    }
    
    private static DeltaConnection connect(DataState dataState, Id datasourceId, DatasetGraph dsg, DeltaLink dLink) {
        if ( ! Objects.equals(datasourceId, dataState.getDataSourceId()) )
            throw new DeltaException("State ds id: "+dataState.getDataSourceId()+" but app passed "+datasourceId);
        DeltaConnection client = new DeltaConnection(dataState, dsg, dLink);
        client.start();
        FmtLog.info(Delta.DELTA_LOG, "%s", client);
        return client;
    }
    
    private static void ensureRegistered(DeltaLink link, Id clientId) {
        if ( ! link.isRegistered() )
            link.register(clientId);
    }
    
    private DeltaConnection(DataState dataState, DatasetGraph dsg, DeltaLink link) {
        if ( dsg instanceof DatasetGraphChanges )
            Log.warn(this.getClass(), "DatasetGraphChanges passed into DeltaClient");
        this.state = dataState;
        this.base = dsg;
        this.datasourceId = dataState.getDataSourceId();
        this.dLink = link;
        
        if ( dsg != null  ) {
            // Where to put incoming changes. 
            this.target = new RDFChangesApply(dsg);
            // Where to send outgoing changes.
            // Make RDFChangesHTTP one shot.
            
            
            // XXX Wrap in version updater 
            RDFChanges monitor = createRDFChanges(datasourceId);
            // 
            this.managed = new DatasetGraphChanges(dsg, monitor);
        } else {
            this.target = null;
            this.managed = null;
        }
    }
    
    private RDFChanges createRDFChanges(Id dsRef) {
        RDFChanges c = new RDFChangesCollector() {
            private Node currentTransactionId = null;
            
            @Override
            public void txnBegin() {
                super.txnBegin();
                if ( currentTransactionId == null ) {
                    currentTransactionId = Id.create().asNode();
                    super.header(RDFPatch.ID, currentTransactionId);
                }
            }

            @Override
            public void txnCommit() {
                super.txnCommit();
                RDFPatch p = getRDFPatch();
                int newVersion = dLink.sendPatch(dsRef, p);
                setLocalVersionNumber(newVersion);
            }
        };
        return c ;
    }
    
    public void start() {
        sync();
    }
    
    public void finish() { }

    public void sync() {
        // [Delta] replace with a one-shot "get all missing patches" operation.

        // Their update id.
        int remoteVer;
        try {
            remoteVer = getRemoteVersionLatest();
        } catch (HttpException ex) {
            // Much the same as : ex.getResponse() == null; HTTP didn't do its thing.
            if ( ex.getCause() instanceof java.net.ConnectException ) {
                FmtLog.warn(LOG, "Failed to connect to get remote version: "+ex.getMessage());
                return;
            }
            if ( ex.getStatusLine() != null ) {
                FmtLog.warn(LOG, "Failed; "+ex.getStatusLine());
                return;
            }
            FmtLog.warn(LOG, "Failed to get remote version: "+ex.getMessage());
            throw ex;
        }

        int localVer = getLocalVersionNumber();

        FmtLog.info(LOG, "Sync: Versions [%d, %d]", localVer, remoteVer);
        // XXX -1 ==> Initialize data.
        if ( localVer < 0 ) {
            FmtLog.warn(LOG, "Sync: **** No initialization");
            localVer = 0 ;
            setLocalVersionNumber(0);
        }
        
        if ( localVer > remoteVer ) 
            FmtLog.info(LOG, "Local version ahead of remote : [local=%d, remote=%d]", localVer, remoteVersion.get());
        if ( localVer >= remoteVer ) {
            //FmtLog.info(LOG, "Versions : [%d, %d]", localVer, remoteVer);
            return;
        }
        // bring up-to-date.
        FmtLog.info(LOG, "Patch range [%d, %d]", localVer+1, remoteVer);
        //IntStream.rangeClosed(localVer+1, remoteVer).forEach((x)->{
        for ( int x = localVer+1 ; x <= remoteVer ; x++) {
            FmtLog.info(LOG, "Sync: patch=%d", x);
            RDFPatch patch = fetchPatch(x);
            if ( patch == null ) { 
                FmtLog.info(LOG, "Sync: patch=%d : not found", x);
                continue;
            }
            RDFChanges c = target;
            if ( true )
                c = DeltaOps.print(c);
            patch.apply(c);
        }
        setRemoteVersionNumber(remoteVer);
        setLocalVersionNumber(remoteVer);
    }

//    public void syncAll() {
//        
//    }
    
    @Override
    public void close() {
        //state.zone().release(state);
    }

    public DeltaLink getLink() {
        return dLink;
    }

    public Id getClientId() {
        return dLink.getClientId();
    }

    public String getInitialStateURL() {
        return dLink.initialState(datasourceId);
    }

    public Id getDatasourceId() {
        return datasourceId;
    }

    public RegToken getRegToken() {
        return dLink.getRegToken();
    }

    /** Actively get the remote version */  
    public int getRemoteVersionLatest() {
        int version = dLink.getCurrentVersion(datasourceId);
        if ( remoteVersion.get() < version )
            remoteVersion.set(version);
        else if ( remoteVersion.get() > version ) 
            FmtLog.warn(LOG, "Remote version behind local tracking of remote version: [%d, %d]", version, remoteVersion.get());
        return version;
    }
    
    /** Return the version of the local data store */ 
    public int getLocalVersionNumber() {
        return state.version();
    }
    
    /** Update the version of the local data store */ 
    public void setLocalVersionNumber(int version) {
        state.updateVersion(version);
    }
    
    /** Return our local track of the remote version */ 
    public int getRemoteVersionNumber() {
        return remoteVersion.get();
    }
    
    /** Update the version of the local belief of remote version */ 
    private void setRemoteVersionNumber(int version) {
        remoteVersion.set(version);
    }

    /** The "record changes" version */  
    public DatasetGraph getDatasetGraph() {
        return managed;
    }

    /** The "without changes" storage */   
    public DatasetGraph getStorage() {
        return base;
    }

    public synchronized void sendPatch(RDFPatch patch) {
        int ver = dLink.sendPatch(datasourceId, patch);
        int ver0 = state.version();
        if ( ver0 > ver )
            FmtLog.warn(LOG, "Version did not advance: %d -> %d", ver0 , ver);
        state.updateVersion(ver);
    }

    private RDFPatch fetchPatch(int id) {
        return dLink.fetch(datasourceId, id);
    }
    
    @Override
    public String toString() {
        return String.format("Data '%s' [local=%d, remote=%d]", datasourceId, 
                             getLocalVersionNumber(), getRemoteVersionNumber());
    }

}
