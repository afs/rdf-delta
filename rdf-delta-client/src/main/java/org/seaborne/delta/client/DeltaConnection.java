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
import java.util.stream.IntStream;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.DeltaOps;
import org.seaborne.delta.Id;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.changes.RDFChangesApply ;
import org.seaborne.patch.system.DatasetGraphChanges;
import org.slf4j.Logger;

/** Provides an interface to a specific dataset over the general {@link DeltaLink} API. 
 * This is the client API, c.f. JDBC connection
 */ 
public class DeltaConnection {
    
    public enum Backing { TDB, FILE }
    
    // Need local version tracking?
    //   yes - for bring DSG up to date.
    
    // Or via DeltaLink?
    
    
    private static Logger LOG = Delta.DELTA_LOG;
    
    // The version of the remote copy.
    
    private final DeltaLink dLink ;
    
    private final AtomicInteger remoteEpoch = new AtomicInteger(0);

    private final DatasetGraph base;
    private final DatasetGraphChanges managed;
    
    private final RDFChanges target;
    private final Id datasourceId;
    private final DataState state;

//    public static DataState init(Location stateArea, Id datasourceId, Backing backing) {
//        // Prepare state area.
//        DataState.format(stateArea, datasourceId, Backing.FILE);
//        DataState dataState = DataState.attach(stateArea, datasourceId);
//        return dataState; 
//    }
    
    /**
     * Create and connect to a new  {@code DataSource}. 
     * Must be registered with the {@code DelatLink}.
     */
    public static DeltaConnection create(Id clientId, Location stateArea, String datasourceName, String uri, DatasetGraph dsg, DeltaLink dLink) {
        Objects.requireNonNull(datasourceName, "Null datasource name");
        Objects.requireNonNull(dLink, "Null link");
        ensureRegistered(dLink, clientId);

        // Create remote.
        Id datasourceId = dLink.newDataSource(datasourceName, uri);
        // Prepare state area.
        
        DataState.format(stateArea, datasourceId, Backing.FILE);
        DataState dataState = DataState.attach(stateArea, datasourceId);

        DeltaConnection client = DeltaConnection.connect(dataState, datasourceId, dsg, dLink);
        return client;
    }

    /** 
     * Connect to an existing {@code DataSource}.
     * Must be registered with the {@code DeltaLink}.
     */  
    public static DeltaConnection connect(Id clientId, Location stateArea, Id datasourceId, DatasetGraph dsg, DeltaLink dLink) {
        Objects.requireNonNull(datasourceId, "Null data source Id");
        Objects.requireNonNull(dLink, "Null link");
        //ensureRegistered(dLink, clientId);
        DataState dataState = DataState.attach(stateArea, datasourceId);
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
            RDFChanges monitor = link.createRDFChanges(datasourceId);
            this.managed = new DatasetGraphChanges(dsg, monitor);
        } else {
            this.target = null;
            this.managed = null;
        }
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

        //FmtLog.info(LOG, "Versions : [%d, %d]", localVer, remoteVer);

        if ( localVer > remoteVer ) 
            FmtLog.info(LOG, "Local version ahead of remote : [local=%d, remote=%d]", state.version(), remoteEpoch.get());
        if ( localVer >= remoteVer ) {
            //FmtLog.info(LOG, "Versions : [%d, %d]", localVer, remoteVer);
            return;
        }
        // bring up-to-date.
        FmtLog.info(LOG, "Patch range [%d, %d]", localVer+1, remoteVer);
        IntStream.rangeClosed(localVer+1, remoteVer).forEach((x)->{
            FmtLog.info(LOG, "Sync: patch=%d", x);
            RDFPatch patch = fetchPatch(x);
            RDFChanges c = target;
            if ( true )
                c = DeltaOps.print(c);
            patch.apply(c);
        });
        setRemoteVersionNumber(remoteVer);
        setLocalVersionNumber(remoteVer);
    }

//    public void syncAll() {
//        
//    }
    
    public DeltaLink getLink() {
        return dLink;
    }

    public Id getClientId() {
        return dLink.getClientId();
    }

    public Id getDatasourceId() {
        return datasourceId;
    }

    public RegToken getRegToken() {
        return dLink.getRegToken();
    }

    /** Actively get the remote version */  
    public int getRemoteVersionLatest() {
        return dLink.getCurrentVersion(datasourceId);
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
        return remoteEpoch.get();
    }
    
    /** Update the version of the local belief of remote version */ 
    private void setRemoteVersionNumber(int version) {
        remoteEpoch.set(version);
    }

    /** The "record changes" version */  
    public DatasetGraph getDatasetGraph() {
        return managed;
    }

    /** The "without changes" storage */   
    public DatasetGraph getStorage() {
        return base;
    }

    private synchronized void sendPatch(RDFPatch patch) {
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
