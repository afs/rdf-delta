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

import static org.seaborne.delta.DeltaConst.VERSION_UNSET ;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference ;
import java.util.function.Consumer ;

import org.apache.jena.atlas.lib.Lib ;
import org.apache.jena.atlas.lib.Pair ;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite ;
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
import org.slf4j.LoggerFactory ;

/** Provides an interface to a specific dataset over the general {@link DeltaLink} API. 
 * This is the client API, c.f. JDBC connection
 */ 
public class DeltaConnection implements AutoCloseable {
    
    private static Logger LOG = LoggerFactory.getLogger(DeltaConnection.class);//Delta.DELTA_LOG;
    
    // The version of the remote copy.
    private final DeltaLink dLink ;

    // Last seen PatchLogInfo
    private final AtomicReference<PatchLogInfo> remote = new AtomicReference<>(null);
    private final DatasetGraph base;
    private final DatasetGraphChanges managed;
    
    private final RDFChanges target;
    private final String datasourceName ;
    private final Id datasourceId;
    private final DataState state;

    private boolean valid = false;
 
    /** 
     * Connect to an existing {@code DataSource} with the {@link DatasetGraph} as local state.
     * The {@code DatasetGraph} must be in-step with the zone.
     * {@code DeltaConnection} objects are normal obtained via {@link DeltaClient}.
     */
    
    /*package*/ static DeltaConnection connect(DataState dataState, DatasetGraph dsg, DeltaLink dLink) {
        Objects.requireNonNull(dataState, "Null data state");
        Objects.requireNonNull(dataState.getDataSourceId(), "Null data source Id");
        Objects.requireNonNull(dataState.getDatasourceName(), "Null data source Id");
        Objects.requireNonNull(dLink, "Null link");
        DeltaConnection client = new DeltaConnection(dataState, dsg, dLink);
        client.start();
        return client;
    }
    
    private static void ensureRegistered(DeltaLink link, Id clientId) {
        if ( ! link.isRegistered() )
            link.register(clientId);
    }
    
    private DeltaConnection(DataState dataState, DatasetGraph basedsg, DeltaLink link) {
        Objects.requireNonNull(dataState, "DataState");
        Objects.requireNonNull(link, "DeltaLink");
        if ( basedsg instanceof DatasetGraphChanges )
            Log.warn(this.getClass(), "DatasetGraphChanges passed into "+Lib.className(this));
        this.state = dataState;
        this.base = basedsg;
        this.datasourceId = dataState.getDataSourceId();
        this.datasourceName = dataState.getDatasourceName();
        this.dLink = link;
        this.valid = true;
        if ( basedsg != null  ) {
            // Where to put incoming changes. 
            this.target = new RDFChangesApply(basedsg);
            // Where to send outgoing changes.
            RDFChanges monitor = createRDFChanges(datasourceId);
            this.managed = new DatasetGraphChanges(basedsg, monitor, null, syncer());
        } else {
            this.target = null;
            this.managed = null;
        }
    }
    
    // If a read, try but carry on.
    private Consumer<ReadWrite> syncer() {
        return (rw)->{
            try { this.sync() ; }
            catch (RuntimeException ex) {
                if ( rw == ReadWrite.WRITE )
                    throw ex;
            }
        };
    }
    
    // clone
    protected DeltaConnection(DeltaConnection other) {
        Objects.nonNull(other);
        this.state = other.state;
        this.base = other.base;
        this.datasourceId = other.datasourceId;
        this.datasourceName = other.datasourceName;
        this.dLink = other.dLink;
        this.valid = other.valid;
        // Not shared with "other".
        if ( base != null  ) {
            this.target = new RDFChangesApply(base);
            RDFChanges monitor = createRDFChanges(datasourceId);
            this.managed = new DatasetGraphChanges(base, monitor, null, syncer());
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
                //FmtLog.info(LOG,  "Send patch: id=%s, prev=%s", Id.str(patch.getId()), Id.str(patch.getPrevious()));
                //long newVersion = dLink.append(dsRef, patch);
                //setLocalState(newVersion, patch.getId());
                append(patch);
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

    /** Send a patch to log server. */
    public synchronized void append(RDFPatch patch) {
        checkDeltaConnection();
        // Autoallocated previous problem. 
        long ver = dLink.append(datasourceId, patch);
        long ver0 = state.version();
        if ( ver0 >= ver )
            FmtLog.warn(LOG, "Version did not advance: %d -> %d", ver0 , ver);
        state.updateState(ver, Id.fromNode(patch.getId()));
    }

    public boolean trySync() {
        try { sync() ; return true ; }
        catch (RuntimeException ex ) { return false ; }
    }
    
    public void sync() {
        checkDeltaConnection();
        long remoteVer = getRemoteVersionLatestOrDefault(VERSION_UNSET);
        if ( remoteVer == VERSION_UNSET ) {
            FmtLog.warn(LOG, "Sync: Failed to sync");
            return;
        }

        long localVer = getLocalVersion();

        // -1 ==> no entries, uninitialized.
        if ( localVer < 0 ) {
            FmtLog.info(LOG, "Sync: No log entries");
            localVer = 0 ;
            setLocalState(localVer, (Node)null);
            return;
        }
        
        if ( localVer > remoteVer ) 
            FmtLog.info(LOG, "Local version ahead of remote : [local=%d, remote=%d]", getLocalVersion(), getRemoteVersionCached());
        if ( localVer >= remoteVer )
            return;
        // bring up-to-date.
        
        FmtLog.info(LOG, "Sync: Versions [%d, %d]", localVer, remoteVer);
        playPatches(localVer+1, remoteVer) ;
        //FmtLog.info(LOG, "Now: Versions [%d, %d]", getLocalVersion(), remoteVer);
    }

    /** Get getRemoteVersionLatest with HTTP handling */
    private long getRemoteVersionLatestOrDefault(long dftValue) {
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

//    /** Play all the patches from the named version to the latested */
//    public void playFrom(int firstVersion) {
//        long remoteVer = getRemoteVersionLatestOrDefault(VERSION_UNSET);
//        if ( remoteVer < 0 ) {
//            FmtLog.warn(LOG, "Sync: Failed to sync");
//            return;
//        }
//        playPatches(firstVersion, remoteVer);
//    }
    
    /** Play the patches (range is inclusive at both ends) */
    private void playPatches(long firstPatchVer, long lastPatchVer) {
        Pair<Long, Node> p = play(datasourceId, target, dLink, firstPatchVer, lastPatchVer);
        long patchLastVersion = p.car();
        Node patchLastIdNode = p.cdr();
        setLocalState(patchLastVersion, patchLastIdNode);
    }
    
    /** Play patches, return details of the the last successfully applied one */ 
    private static Pair<Long, Node> play(Id datasourceId, RDFChanges target, DeltaLink dLink, long minVersion, long maxVersion) {
        // [Delta] replace with a one-shot "get all patches" operation.
        //FmtLog.debug(LOG, "Patch range [%d, %d]", minVersion, maxVersion);
        Node patchLastIdNode = null;
        long patchLastVersion = VERSION_UNSET;
        
        for ( long ver = minVersion ; ver <= maxVersion ; ver++ ) {
            //FmtLog.debug(LOG, "Play: patch=%d", ver);
            RDFPatch patch;
            try { 
                patch = dLink.fetch(datasourceId, ver);
                if ( patch == null ) { 
                    FmtLog.info(LOG, "Play: %s patch=%d : not found", datasourceId, ver);
                    continue;
                }
            } catch (DeltaNotFoundException ex) {
                // Which ever way it is signalled.  This way means "bad datasourceId"
                FmtLog.info(LOG, "Play: %s patch=%d : not found", datasourceId, ver);
                continue;
            }
            RDFChanges c = target;
            if ( false )
                c = DeltaOps.print(c);
            patch.apply(c);
            patchLastIdNode = patch.getId();
            patchLastVersion = ver;
        }
        return Pair.create(patchLastVersion, patchLastIdNode);
    }
    
    @Override
    public void close() {
        // Return to pool if pooled.
    }
    
    public boolean isValid() {
        return valid;
    }

    public DeltaLink getLink() {
        return dLink;
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
    
    /** Actively get the remote version */
    public long getRemoteVersionLatest() {
        checkDeltaConnection();
        PatchLogInfo info = getPatchLogInfo();
        if ( info == null )
            return VERSION_UNSET;
        return info.getMaxVersion();
    }
    
    /** Return the version of the local data store */ 
    public long getLocalVersion() {
        checkDeltaConnection();
        return state.version();
    }
    
    /** Return the version of the local data store */ 
    public Id getLatestPatchId() {
        checkDeltaConnection();
        return state.latestPatchId();
    }

    /** Actively get the remote latest patch id */
    public Id getRemotePatchId() {
        checkDeltaConnection();
        return getPatchLogInfo().getLatestPatch();
    }

    /** Update the version of the local data store */ 
    private void setLocalState(long version, Node patchId) {
        setLocalState(version, (patchId == null) ? null : Id.fromNode(patchId));
    }
    
    /** Update the version of the local data store */ 
    private void setLocalState(long version, Id patchId) {
        state.updateState(version, patchId);
    }

    /** Return our local track of the remote version */ 
    private long getRemoteVersionCached() {
        //checkDeltaConnection();
        if ( remote.get() == null )
            return VERSION_UNSET; 
        return (int)remote.get().getMaxVersion();
    }

    /** The "record changes" version */  
    public DatasetGraph getDatasetGraph() {
        checkDeltaConnection();
        return managed;
    }

    /** The "without changes" storage */   
    public DatasetGraph getStorage() {
        return base;
    }

    @Override
    public String toString() {
        String str = String.format("DConn %s [local=%d, remote=%d]", datasourceId, getLocalVersion(), getRemoteVersionCached());
        if ( ! valid )
            str = str + " : invalid";
        return str;
    }

}
