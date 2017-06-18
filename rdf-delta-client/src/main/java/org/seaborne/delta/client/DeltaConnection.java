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
import java.util.concurrent.atomic.AtomicReference ;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.system.Txn ;
import org.seaborne.delta.*;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.changes.RDFChangesApply ;
import org.seaborne.patch.changes.RDFChangesCollector;
import org.seaborne.patch.system.DatasetGraphChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory ;

/** Provides an interface to a specific dataset over the general {@link DeltaLink} API. 
 * This is the client API, c.f. JDBC connection
 */ 
public class DeltaConnection implements AutoCloseable {
    
    public enum Backing { TDB, FILE }
    private static Logger LOG = LoggerFactory.getLogger(DeltaConnection.class);//Delta.DELTA_LOG;
    
    // The version of the remote copy.
    private final DeltaLink dLink ;
    private final Zone zone ;

    // Last seen PatchLogInfo
    private final AtomicReference<PatchLogInfo> remote = new AtomicReference<>(null);
    private final DatasetGraph base;
    private final DatasetGraphChanges managed;
    
    private final RDFChanges target;
    private final Id datasourceId;
    private final DataState state;

    private boolean valid;

    /**
     * Create and connect to a new {@code DataSource}. 
     * The caller must be registered with the {@code DeltaLink}.
     */
    public static DeltaConnection create(Zone zone, String datasourceName, String uri, DatasetGraph dsg, DeltaLink dLink) {
        Objects.requireNonNull(datasourceName, "Null datasource name");
        Objects.requireNonNull(dLink, "Null link");
        
        Id datasourceId = dLink.newDataSource(datasourceName, uri);
        // Inital data.
        DataState dataState = zone.create(datasourceId, datasourceName, uri, Backing.TDB);
        DeltaConnection client = DeltaConnection.connect$(zone, dataState, datasourceId, dsg, dLink);
        return client;
    }

    /** 
     * Connect to an existing {@code DataSource} with a fresh {@link DatasetGraph} as local state.
     * The {@code DatasetGraph} is assumed to empty and is brought up-to-date.
     * See {@link DeltaConnection#connect} for connecting an existing local dataset.   
     * The client must be registered with the {@code DeltaLink}.
     */
    public static DeltaConnection attach(Zone zone, Id datasourceId, DatasetGraph dsg, DeltaLink dLink) {
        Objects.requireNonNull(datasourceId, "Null data source Id");
        Objects.requireNonNull(dLink, "Null link");
        //DataSourceDescription dsd = dLink.getDataSourceDescription(datasourceId);
        spinUpDSG(datasourceId, dsg, dLink);
        // If Zone is behind, no problem - patches will be replayed.
        DeltaConnection dConn = DeltaConnection.connect(zone, datasourceId, dsg, dLink);
        return dConn;
    }
    
    private static void spinUpDSG(Id datasourceId, DatasetGraph dsg, DeltaLink dLink) {
        String url = dLink.initialState(datasourceId);
        if ( url != null )
            Txn.executeWrite(dsg, ()->RDFDataMgr.read(dsg, url));  
        PatchLogInfo logInfo = dLink.getPatchLogInfo(datasourceId);
        // XXX Check with zone.
        play(datasourceId, dsg, dLink, (int)logInfo.getMinVersion(), (int)logInfo.getMaxVersion());
    }
    
    // XXX DRY See playPatches.
    private static void play(Id datasourceId, DatasetGraph dsg, DeltaLink dLink, int minVersion, int maxVersion) {
        RDFChanges target = new RDFChangesApply(dsg);
        Node patchLastIdNode = null;
        for ( int i = minVersion ; i <= maxVersion ; i++ ) {
            FmtLog.info(LOG, "Attach: patch=%d", i);
            RDFPatch patch = dLink.fetch(datasourceId, i);
            if ( patch == null ) { 
                FmtLog.info(LOG, "Sync: patch=%d : not found", i);
                continue;
            }
            RDFChanges c = target;
            if ( false )
                c = DeltaOps.print(c);
            patch.apply(c);
            patchLastIdNode = patch.getId();
        }
    }

    /** 
     * Connect to an existing {@code DataSource} with the {@link DatasetGraph} as local state.
     * The  {@code DatasetGraph} must be in-step with the zone.
     * See {@link DeltaConnection#attach} for introducing a new dataset.   
     * The client must be registered with the {@code DeltaLink}.
     */  
    public static DeltaConnection connect(Zone zone, Id datasourceId, DatasetGraph dsg, DeltaLink dLink) {
        Objects.requireNonNull(datasourceId, "Null data source Id");
        Objects.requireNonNull(dLink, "Null link");
        
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
            DataState dataState = zone.create(datasourceId, dsd.getName(), dsd.getUri(), Backing.TDB);
            DeltaConnection dConn = DeltaConnection.connect$(zone, dataState, datasourceId, dsg, dLink);
            return dConn;
        }
        // Disk refresh.
        zone.refresh(datasourceId);
        DataState dataState = zone.attach(datasourceId);
        if ( ! Objects.equals(datasourceId, dataState.getDataSourceId()) )
            throw new DeltaException("State ds "+dataState.getDataSourceId()+" but app passed "+datasourceId);
        DeltaConnection client = DeltaConnection.connect$(zone, dataState, datasourceId, dsg, dLink);
        return client;
    }
    
    /* Common code to create the local DeltaConnection and set it up. */
    private static DeltaConnection connect$(Zone zone,DataState dataState, Id datasourceId, DatasetGraph dsg, DeltaLink dLink) {
        if ( ! Objects.equals(datasourceId, dataState.getDataSourceId()) )
            throw new DeltaException("State ds id: "+dataState.getDataSourceId()+" but app passed "+datasourceId);
        DeltaConnection client = new DeltaConnection(zone, dataState, dsg, dLink);
        client.start();
        FmtLog.info(Delta.DELTA_LOG, "%s", client);
        return client;
    }
    
    private static void ensureRegistered(DeltaLink link, Id clientId) {
        if ( ! link.isRegistered() )
            link.register(clientId);
    }
    
    private DeltaConnection(Zone zone, DataState dataState, DatasetGraph dsg, DeltaLink link) {
        if ( dsg instanceof DatasetGraphChanges )
            Log.warn(this.getClass(), "DatasetGraphChanges passed into DeltaClient");
        this.state = dataState;
        this.base = dsg;
        this.datasourceId = dataState.getDataSourceId();
        this.dLink = link;
        this.zone = zone;
        this.valid = true;
        
        if ( dsg != null  ) {
            // Where to put incoming changes. 
            this.target = new RDFChangesApply(dsg);
            // Where to send outgoing changes.
            // Make RDFChangesHTTP one shot.
            // XXX Wrap in version updater 
            RDFChanges monitor = createRDFChanges(datasourceId);
            this.managed = new DatasetGraphChanges(dsg, monitor);
        } else {
            this.target = null;
            this.managed = null;
        }
    }
    
    protected DeltaConnection(DeltaConnection other) {
        this.state = other.state;
        this.base = other.base;
        this.datasourceId = other.datasourceId;
        this.dLink = other.dLink;
        this.zone = other.zone;
        this.valid = other.valid;
        // Not shared with "other".
        if ( base != null  ) {
            this.target = new RDFChangesApply(base);
            // Where to send outgoing changes.
            // Make RDFChangesHTTP one shot.
            // XXX Wrap in version updater 
            RDFChanges monitor = createRDFChanges(datasourceId);
            this.managed = new DatasetGraphChanges(base, monitor);
        } else {
            this.target = null;
            this.managed = null;
        }
    }
    
    private void checkDeltaConnection() {
        if ( ! valid )
            throw new DeltaConfigException("DeltaConnection not valid");
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
                if ( super.header(RDFPatch.PREV) == null ) {
                    Id x = state.latestPatchId();
                    if ( x != null )
                        super.header(RDFPatch.PREV, x.asNode());
                }

                RDFPatch patch = getRDFPatch();
                FmtLog.info(LOG,  "Send patch: id=%s, prev=%s", patch.getId(), patch.getPrevious());
                int newVersion = dLink.append(dsRef, patch);
                setLocalState(newVersion, patch.getId());
                currentTransactionId = null;
                reset();
            }
            
            @Override
            public void txnAbort() { reset() ; }
//            @Override
//            public void finish() { reset() ; } 
        };
        return c ;
    }
    
    /*package*/ void start() {
        checkDeltaConnection();
        sync();
    }
    
    /*package*/ void finish() { }

    public void sync() {
        checkDeltaConnection();
        int remoteVer = getRemoteVersionLatestOrDefault(DeltaConst.VERSION_UNSET);
        if ( remoteVer == DeltaConst.VERSION_UNSET ) {
            FmtLog.warn(LOG, "Sync: Failed to sync");
            return;
        }

        int localVer = getLocalVersion();

        FmtLog.info(LOG, "Sync: Versions [%d, %d]", localVer, remoteVer);
        // XXX -1 ==> Initialize data.
        if ( localVer < 0 ) {
            FmtLog.warn(LOG, "Sync: **** No initialization");
            localVer = 0 ;
            setLocalState(0, (Node)null);
        }
        
        if ( localVer > remoteVer ) 
            FmtLog.info(LOG, "Local version ahead of remote : [local=%d, remote=%d]", getLocalVersion(), getRemoteVersionCached());
        if ( localVer >= remoteVer ) {
            //FmtLog.info(LOG, "Versions : [%d, %d]", localVer, remoteVer);
            return;
        }
        // bring up-to-date.
        
        playPatches(localVer+1, remoteVer) ;
    }

    /** Get getRemoteVersionLatest with HTTP handling */
    private int getRemoteVersionLatestOrDefault(int dftValue) {
        try {
            return getRemoteVersionLatest();
        } catch (HttpException ex) {
            // Much the same as : ex.getResponse() == null; HTTP didn't do its thing.
            if ( ex.getCause() instanceof java.net.ConnectException ) {
                FmtLog.warn(LOG, "Failed to connect to get remote version: "+ex.getMessage());
                return dftValue;
            }
            if ( ex.getStatusLine() != null ) {
                FmtLog.warn(LOG, "Failed; "+ex.getStatusLine());
                return dftValue;
            }
            FmtLog.warn(LOG, "Failed to get remote version: "+ex.getMessage());
            throw ex;
        }
    }

    /** Play all the patches from the named version to the latested */
    public void playFrom(int firstVersion) {
        int remoteVer = getRemoteVersionLatestOrDefault(DeltaConst.VERSION_UNSET);
        if ( remoteVer < 0 ) {
            FmtLog.warn(LOG, "Sync: Failed to sync");
            return;
        }
        playPatches(firstVersion, remoteVer);
    }
    
    /** Play the patches (range is inclusive at both ends) */
    private void playPatches(int firstPatchVer, int lastPatchVer) {
        // [Delta] replace with a one-shot "get all patches" operation.
        FmtLog.info(LOG, "Patch range [%d, %d]", firstPatchVer, lastPatchVer);
        //IntStream.rangeClosed(localVer+1, remoteVer).forEach((x)->{
        
        Node patchLastId = null;
        
        for ( int x = firstPatchVer ; x <= lastPatchVer ; x++) {
            FmtLog.info(LOG, "Sync: patch=%d", x);
            RDFPatch patch = fetchPatch(x);
            if ( patch == null ) { 
                FmtLog.info(LOG, "Sync: patch=%d : not found", x);
                continue;
            }
            RDFChanges c = target;
            if ( false )
                c = DeltaOps.print(c);
            patch.apply(c);
            patchLastId = patch.getId();
        }
        setLocalState(lastPatchVer, patchLastId);
    }

    @Override
    public void close() {
    }
    
    public boolean isValid() {
        return valid;
    }

    public void removeDataSource() {
        checkDeltaConnection();
        valid = false;
        dLink.removeDataSource(datasourceId);
        zone.delete(datasourceId);
    }

    public DeltaLink getLink() {
        return dLink;
    }

    public Zone getZone() {
        return zone;
    }

    public Id getClientId() {
        return dLink.getClientId();
    }

    public String getInitialStateURL() {
        checkDeltaConnection();
        return dLink.initialState(datasourceId);
    }

    public Id getDataSourceId() {
        checkDeltaConnection();
        return datasourceId;
    }

    public PatchLogInfo getPatchLogInfo() {
        checkDeltaConnection();
        PatchLogInfo info = dLink.getPatchLogInfo(datasourceId);
        if ( info != null ) {
            if ( remote.get() != null ) {
                if ( getRemoteVersionCached() > 0 && info.getMaxVersion() < getRemoteVersionCached() )
                    FmtLog.warn(LOG, "Remote version behind local tracking of remote version: [%d, %d]", info.getMaxVersion(), getRemoteVersionCached());
            }
            // Set the local copy whenever we get the remote latest.
            remote.set(info);
        }
        return info;
    }

    public RegToken getRegToken() {
        checkDeltaConnection();
        return dLink.getRegToken();
    }

    /** Actively get the remote log latest id */  
    public Id getRemoteIdLatest() {
        checkDeltaConnection();
        PatchLogInfo logInfo = dLink.getPatchLogInfo(datasourceId);
        
        if ( logInfo == null ) {
            // Can this happen? Deleted datasourceId?
            FmtLog.warn(LOG, "Failed to get remote latest patchId");
            return null;
        }
        return logInfo.getLatestPatch();
    }
    
    // XXX remove and only have PatchLogInfo. 
    /** Actively get the remote version */
    public int getRemoteVersionLatest() {
        checkDeltaConnection();
        PatchLogInfo info = getPatchLogInfo();
        if ( info == null )
            return DeltaConst.VERSION_UNSET;
        return (int)info.getMaxVersion();
    }
    
    /** Return the version of the local data store */ 
    public int getLocalVersion() {
        checkDeltaConnection();
        return state.version();
    }
    
    /** Return the version of the local data store */ 
    public Id getLatestPatchId() {
        checkDeltaConnection();
        return state.latestPatchId();
    }

    // XXX remove and only have PatchLogInfo. 
    /** Actively get the remote latest patch id */
    public Id getRemotePatchId() {
        checkDeltaConnection();
        return dLink.getPatchLogInfo(datasourceId).getLatestPatch();
    }

        /** Update the version of the local data store */ 
    private void setLocalState(int version, Node patchId) {
        setLocalState(version, (patchId == null) ? null : Id.fromNode(patchId));
    }
    
    /** Update the version of the local data store */ 
    private void setLocalState(int version, Id patchId) {
        state.updateState(version, patchId);
    }

    /** Return our local track of the remote version */ 
    private int getRemoteVersionCached() {
        //checkDeltaConnection();
        if ( remote.get() == null )
            return DeltaConst.VERSION_UNSET; 
        return (int)remote.get().getMaxVersion();
    }
//    
//    /** Update the version of the local belief of remote version */ 
//    private void setRemoteVersionNumber(int version) {
//        checkDeltaConnection();
//        remoteVersion.set(version);
//    }

    /** The "record changes" version */  
    public DatasetGraph getDatasetGraph() {
        checkDeltaConnection();
        return managed;
    }

    /** The "without changes" storage */   
    public DatasetGraph getStorage() {
        return base;
    }

    public synchronized void append(RDFPatch patch) {
        checkDeltaConnection();
        // Autoallocated previous problem. 
        int ver = dLink.append(datasourceId, patch);
        int ver0 = state.version();
        if ( ver0 >= ver )
            FmtLog.warn(LOG, "Version did not advance: %d -> %d", ver0 , ver);
        state.updateState(ver, Id.fromNode(patch.getId()));
    }

    private RDFPatch fetchPatch(int id) {
        return dLink.fetch(datasourceId, id);
    }
    
    @Override
    public String toString() {
        String str = String.format("DConn '%s' [local=%d, remote=%d]", datasourceId, getLocalVersion(), getRemoteVersionCached());
        if ( ! valid )
            str = str + " : invalid";
        return str;
    }

}
