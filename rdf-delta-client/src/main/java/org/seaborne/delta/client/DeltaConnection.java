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

import static java.lang.String.format;
import static org.seaborne.delta.DeltaConst.VERSION_UNSET ;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference ;
import java.util.function.Consumer ;

import org.apache.jena.atlas.lib.Lib ;
import org.apache.jena.atlas.lib.Pair ;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite ;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.web.HttpSC;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.DeltaNotFoundException;
import org.seaborne.delta.DeltaOps;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.RDFPatchConst;
import org.seaborne.patch.changes.RDFChangesApply ;
import org.seaborne.patch.changes.RDFChangesCollector;
import org.seaborne.patch.system.DatasetGraphChanges;
import org.seaborne.patch.system.RDFChangesSuppressEmpty;
import org.slf4j.Logger;

/** Provides an interface to a specific dataset over the general {@link DeltaLink} API. 
 * This is the client API, c.f. JDBC connection
 */ 
public class DeltaConnection implements AutoCloseable {
    
    private static Logger LOG = Delta.DELTA_CLIENT;
    
    // The version of the remote copy.
    private final DeltaLink dLink ;

    // Last seen PatchLogInfo
    private final AtomicReference<PatchLogInfo> remote = new AtomicReference<>(null);
    private final DatasetGraph base;
    private final DatasetGraphChanges managed;
    private final Dataset managedDataset;

    // Suppressed emopty commits versions
    private final DatasetGraphChanges managedNoEmpty;
    private final Dataset managedNoEmptyDataset;

    
    private final RDFChanges target;
    private final String datasourceName ;
    private final Id datasourceId;
    // Note: the contents of DataState change- it is the current state and is updated. 
    private final DataState state;

    private boolean valid = false;
    private final TxnSyncPolicy syncTxnBegin;
    
    /** 
     * Connect to an existing {@code DataSource} with the {@link DatasetGraph} as local state.
     * The {@code DatasetGraph} must be in-step with the zone.
     * {@code DeltaConnection} objects are normal obtained via {@link DeltaClient}.
     * {@code TxnSyncPolicy} controls when the {@code DeltaConnection} synchronizes with the patch log.
     */
    
    /*package*/ static DeltaConnection create(DataState dataState, DatasetGraph dsg, DeltaLink dLink, TxnSyncPolicy syncTxnBegin) {
        Objects.requireNonNull(dataState,           "Null data state");
        Objects.requireNonNull(dLink,               "DeltaLink is null");
        Objects.requireNonNull(syncTxnBegin,        "SyncPolicy is null");
        Objects.requireNonNull(dataState.getDataSourceId(), "Null data source Id");
        Objects.requireNonNull(dataState.getDatasourceName(), "Null data source name");
        
        DeltaConnection dConn = new DeltaConnection(dataState, dsg, dLink, syncTxnBegin);
        dConn.start();
        return dConn;
    }
    
    private static void ensureRegistered(DeltaLink link, Id clientId) {
        if ( ! link.isRegistered() )
            link.register(clientId);
    }
    
    private DeltaConnection(DataState dataState, DatasetGraph basedsg, DeltaLink link, TxnSyncPolicy syncTxnBegin) {
        Objects.requireNonNull(dataState, "DataState");
        Objects.requireNonNull(link, "DeltaLink");
        //Objects.requireNonNull(basedsg, "base DatasetGraph");
        if ( basedsg instanceof DatasetGraphChanges )
            FmtLog.warn(this.getClass(), "[%s] DatasetGraphChanges passed into %s", dataState.getDataSourceId() ,Lib.className(this));
        this.state = dataState;
        this.base = basedsg;
        this.datasourceId = dataState.getDataSourceId();
        this.datasourceName = dataState.getDatasourceName();
        this.dLink = link;
        this.valid = true;
        this.syncTxnBegin = syncTxnBegin;
        if ( basedsg == null ) {
            this.target = null;
            this.managed = null;
            this.managedDataset = null;
            this.managedNoEmpty = null;
            this.managedNoEmptyDataset = null;
            return;
        }
        
        // Where to put incoming changes. 
        this.target = new RDFChangesApply(basedsg);
        // Where to send outgoing changes.
        RDFChanges monitor = createRDFChanges(datasourceId);
        this.managed = new DatasetGraphChanges(basedsg, monitor, null, syncer(syncTxnBegin));
        this.managedDataset = DatasetFactory.wrap(managed);
        // ----
        RDFChanges monitor1 = new RDFChangesSuppressEmpty(monitor);
        this.managedNoEmpty = new DatasetGraphChanges(basedsg, monitor1, null, syncer(syncTxnBegin));
        this.managedNoEmptyDataset = DatasetFactory.wrap(managedNoEmpty);
    }
    
    private Consumer<ReadWrite> syncer(TxnSyncPolicy syncTxnBegin) {
        switch(syncTxnBegin) {
            case NONE :     return (rw)->{} ; 
            case TXN_RW :   return syncerTxnBeginRW();
            case TXN_W :    return syncerTxnBeginW();
            default :       throw new IllegalStateException();
        }
    }
    
    /** Sync on transaction begin.
     * <p>
     *  READ -> attempt to sync, WRITE -> not silently on errors. 
     */
    private Consumer<ReadWrite> syncerTxnBeginRW() { 
        return (rw)->{
            switch(rw) {
                case READ: 
                    try { sync(); } catch (Exception ex) {}
                    break;
                case WRITE:
                    this.sync();
                    break;
            }
        };
    }
    
    /** Sync on W transaction begin, not on R
     * <p>
     *  READ -> op, WRITE -> call {@code .sync()}. 
     */
    private Consumer<ReadWrite> syncerTxnBeginW() { 
        return (rw)->{
            switch(rw) {
                case READ:// No action. 
                    break;
                case WRITE:
                    this.sync();
                    break;
            }
        };
    }

    private void checkDeltaConnection() {
        if ( ! valid )
            throw new DeltaConfigException(format("[%s] DeltaConnection not valid", datasourceId));
    }
    
    /**
     * An {@link RDFChanges} that adds "id", and "prev" as necessary.
     */
    private class RDFChangesDS extends RDFChangesCollector {
        private Node currentTransactionId = null;
        
        RDFChangesDS() {}
        
        // Auto-add an id.
        @Override
        public void txnBegin() {
            super.txnBegin();
            if ( currentTransactionId == null ) {
                currentTransactionId = Id.create().asNode();
                super.header(RDFPatchConst.ID, currentTransactionId);
            }
        }

        // Auto-add previous id.
        @Override
        public void txnCommit() {
            super.txnCommit();
            if ( currentTransactionId == null ) {
                throw new DeltaException(format("[%s] No id in txnCommit - either txnBegin not called or txnCommit called twice", datasourceId));
            }
            if ( super.header(RDFPatchConst.PREV) == null ) {
                Id x = state.latestPatchId();
                if ( x != null )
                    super.header(RDFPatchConst.PREV, x.asNode());
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
        public void txnAbort() {
            super.txnAbort();
            currentTransactionId = null;
            reset();
        }
    }
    
    private RDFChanges createRDFChanges(Id dsRef) {
        RDFChanges changes = new RDFChangesDS();
        //changes = new RDFChangesSuppressEmpty(changes);
        return changes; 
    }
    
    /*package*/ void start() {
        checkDeltaConnection();
        sync();
    }
    
    /*package*/ void finish() { /*reset();*/ }

    /** Send a patch to log server. */
    public synchronized void append(RDFPatch patch) {
        checkDeltaConnection();
        long ver = dLink.append(datasourceId, patch);
        if ( ver < 0 )
            // Didn't happen.
            return ;
        long ver0 = state.version();
        if ( ver0 >= ver )
            FmtLog.warn(LOG, "[%s] Version did not advance: %d -> %d", datasourceId.toString(), ver0 , ver);
        state.updateState(ver, Id.fromNode(patch.getId()));
    }

    public boolean trySync() {
        return attempt(()->sync());
    }
    
    public boolean trySync(PatchLogInfo logInfo) {
        return attempt(()->sync(logInfo));
    }

    public void sync(PatchLogInfo logInfo) {
        checkDeltaConnection();
        syncToVersion(logInfo.getMaxVersion());
    }
    
    public void sync() {
        try { 
            checkDeltaConnection();
            PatchLogInfo logInfo = getPatchLogInfo();
            sync(logInfo);
        } catch (HttpException ex) {
            if ( ex.getResponseCode() == -1 )
                throw new HttpException(HttpSC.SERVICE_UNAVAILABLE_503, HttpSC.getMessage(HttpSC.SERVICE_UNAVAILABLE_503), ex.getMessage());
            throw ex;
        }
    }
    
    // Attempt an operation and return true/false as to whether it succeeded or not.
    private boolean attempt(Runnable action) {
        try { action.run(); return true ; }
        catch (RuntimeException ex ) { return false ; }
//      } catch (HttpException ex) {
//      // Much the same as : ex.getResponse() == null; HTTP didn't do its thing.
//      if ( ex.getCause() instanceof java.net.ConnectException ) {
//          FmtLog.warn(LOG, "Failed to sync: "+ex.getMessage());
//          return;
//      }
//      if ( ex.getStatusLine() != null ) {
//          FmtLog.warn(LOG, "Failed: "+ex.getStatusLine());
//          return;
//      }
//      FmtLog.warn(LOG, "Failed to get remote PatchLogInfo: "+ex.getMessage());
//      throw ex;
//  }
    }
    
    /** Sync until some version */
    private void syncToVersion(long version) {
        //long remoteVer = getRemoteVersionLatestOrDefault(VERSION_UNSET);
        if ( version == VERSION_UNSET ) {
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
        
        if ( localVer > version ) 
            FmtLog.info(LOG, "[%s] Local version ahead of remote : [local=%d, remote=%d]", datasourceId, getLocalVersion(), getRemoteVersionCached());
        if ( localVer >= version )
            return;
        // bring up-to-date.
        
        FmtLog.info(LOG, "Sync: Versions [%d, %d]", localVer, version);
        playPatches(localVer+1, version) ;
        //FmtLog.info(LOG, "Now: Versions [%d, %d]", getLocalVersion(), remoteVer);
    }

//    /** Get getRemoteVersionLatest with HTTP handling */
//    private long getRemoteVersionLatestOrDefault(long dftValue) {
//        try {
//            return getRemoteVersionLatest();
//        } catch (HttpException ex) {
//            // Much the same as : ex.getResponse() == null; HTTP didn't do its thing.
//            if ( ex.getCause() instanceof java.net.ConnectException ) {
//                FmtLog.warn(LOG, "Failed to connect to get remote version: "+ex.getMessage());
//                return dftValue;
//            }
//            if ( ex.getStatusLine() != null ) {
//                FmtLog.warn(LOG, "Failed; "+ex.getStatusLine());
//                return dftValue;
//            }
//            FmtLog.warn(LOG, "Failed to get remote version: "+ex.getMessage());
//            throw ex;
//        }
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
                    FmtLog.warn(LOG, "[%s] Remote version behind local tracking of remote version: [%d, %d]", datasourceId, info.getMaxVersion(), getRemoteVersionCached());
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

    /** The "record changes" version */  
    public Dataset getDataset() {
        return managedDataset;
    }

    /** The "record changes" version, suppresses empty commits on the RDFChanges.
     * @see RDFChangesSuppressEmpty  
     */
    public DatasetGraph getDatasetGraphNoEmpty() {
        checkDeltaConnection();
        return managedNoEmpty;
    }

    /** The "record changes" version, suppresses empty commits on the RDFChanges.
     * @see RDFChangesSuppressEmpty  
     */
    public Dataset getDatasetNoEmpty() {
        return managedNoEmptyDataset;
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
