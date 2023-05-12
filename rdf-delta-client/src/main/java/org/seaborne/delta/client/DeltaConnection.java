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

import static java.lang.String.format;
import static org.seaborne.delta.client.DeltaClientLib.threadFactoryDaemon;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.jena.system.Txn;
import org.apache.jena.web.HttpSC;
import org.seaborne.delta.*;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.DeltaLinkListener;
import org.apache.jena.rdfpatch.RDFChanges;
import org.apache.jena.rdfpatch.RDFPatch ;
import org.apache.jena.rdfpatch.RDFPatchConst;
import org.apache.jena.rdfpatch.changes.RDFChangesApply ;
import org.apache.jena.rdfpatch.changes.RDFChangesCollector;
import org.apache.jena.rdfpatch.changes.RDFChangesExternalTxn;
import org.apache.jena.rdfpatch.system.DatasetGraphChanges;
import org.apache.jena.rdfpatch.system.RDFChangesSuppressEmpty;
import org.slf4j.Logger;

/**
 * Provides an interface to a specific dataset over the general {@link DeltaLink} API.
 * This is the client API, c.f. JDBC connection
 */
public class DeltaConnection implements AutoCloseable {

    private static Logger LOG = Delta.DELTA_CLIENT;

    // The version of the remote copy.
    private final DeltaLink dLink ;

    /**
     * Locking in begin => true.
     * Optimistic transactions, no locking => set False
     */
    private static final boolean LockMode = true;

    // Last seen PatchLogInfo in getPatchLogInfo()
    // null when not started
    private final AtomicReference<PatchLogInfo> remote = new AtomicReference<>(null);
    private final DatasetGraph base;
    private final DatasetGraphChanges managed;
    private final Dataset managedDataset;

    // Suppressed empty commits versions
    private final DatasetGraphChanges managedNoEmpty;
    private final Dataset managedNoEmptyDataset;

    private final RDFChanges target;
    private final String datasourceName ;
    private final Id datasourceId;
    // Note: the contents of DataState change - it is the current state and is updated.
    private final DataState state;
    private final LogLock logLock;

    private boolean valid = false;
    private final SyncPolicy syncPolicy;

    private final LogLockMgr logLockMgr;
    // Synchronization within this DeltaConnection.
    private final Object localLock;

    // Indicator of whether a sync is in-progress.
    private final AtomicBoolean syncInProgress = new AtomicBoolean(false);

    // Synchronize patches asynchronously to the caller.
    // (ExecutorService in case we change the policy details in the future.)

    // Test start and stop servers very quickly and ports are known (in assembler files)
    // Test: TestDeltaAssembler.assembler_delta_3
    public static boolean TestModeNoAsync = false;

    private ScheduledExecutorService scheduledExecutionService = Executors.newScheduledThreadPool(1, threadFactoryDaemon);

    /**
     * Connect to an existing {@code DataSource} with the {@link DatasetGraph} as local state.
     * The {@code DatasetGraph} must be in-step with the zone.
     * {@code DeltaConnection} objects are normal obtained via {@link DeltaClient}.
     * {@code TxnSyncPolicy} controls when the {@code DeltaConnection} synchronizes with the patch log.
     */

    /*package*/ static DeltaConnection create(DataState dataState, DatasetGraph dsg, DeltaLink dLink, SyncPolicy syncTxnBegin) {
        Objects.requireNonNull(dataState,           "Null data state");
        Objects.requireNonNull(dLink,               "DeltaLink is null");
        Objects.requireNonNull(syncTxnBegin,        "SyncPolicy is null");
        Objects.requireNonNull(dataState.getDataSourceId(),     "Null data source Id");
        Objects.requireNonNull(dataState.getDatasourceName(),   "Null data source name");

        DeltaConnection dConn = new DeltaConnection(dataState, dsg, dLink, syncTxnBegin);
        dConn.start();
        return dConn;
    }

    private DeltaConnection(DataState dataState, DatasetGraph basedsg, DeltaLink link, SyncPolicy syncTxnBegin) {
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
        this.logLock = new LogLock(link, datasourceId);

        this.logLockMgr = new LogLockMgr(dLink);
        logLockMgr.start();
        logLockMgr.add(logLock);

        this.valid = true;
        this.localLock = new Object();
        this.syncPolicy = syncTxnBegin;
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

        // Note: future possibility of having RDFChangesEvents
//        RDFChanges t = new RDFChangesApply(basedsg);
//        //t = new RDFChangesEvents(t, n->{System.out.println("**** Event: "+n); return null;});
//        this.target = t;

        // Where to send outgoing changes.
        RDFChanges monitor = new RDFChangesDS();
        this.managed = new DatasetGraphChanges(basedsg, monitor, null, syncer(syncTxnBegin));
        this.managedDataset = DatasetFactory.wrap(managed);
        // ----
        RDFChanges monitor1 = new RDFChangesSuppressEmpty(monitor);
        this.managedNoEmpty = new DatasetGraphChanges(basedsg, monitor1, null, syncer(syncTxnBegin));
        this.managedNoEmptyDataset = DatasetFactory.wrap(managedNoEmpty);
    }

    private Consumer<ReadWrite> syncer(SyncPolicy syncTxnBegin) {
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
     * Acquire the patch log lock else bail out.
     */
    public boolean acquireLock() {
        if ( ! LockMode )
            // No cluster lock then optimistic (can fail) transactions.
            return true;
        checkDeltaConnection();
        return logLock.acquireLock();
    }

    /**
     * Release the patch log lock.
     * If there is an error, the patch service is probably down (single server version).
     * All we can do is ignore, and resync later.
     */
    public void releaseLock() {
        if ( ! LockMode )
            return;
        checkDeltaConnection();
        logLock.releaseLock();
    }

    /**
     * An {@link RDFChanges} that adds "id", and "prev" as necessary.
     */
    private class RDFChangesDS extends RDFChangesCollector {
        private volatile Node currentTransactionId = null;

        RDFChangesDS() {}

        // Auto-add an id.
        @Override
        public void txnBegin() {
            // Without the acquireLock, the transaction is performed optimistically
            // and may fail at the commit if another machine has sneaked in,
            // doing a W transaction and making a log append.
            if ( LockMode ) {
                boolean b = acquireLock();
                if ( ! b )
                    throw new DeltaException("Can't obtain the cluster lock (cluster busy?)");
            }
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
            if ( currentTransactionId == null )
                throw new DeltaException(format("[%s] No id in txnCommit - either txnBegin not called or txnCommit called twice", datasourceId));
            if ( super.header(RDFPatchConst.PREV) == null ) {
                Id x = state.latestPatchId();
                if ( x != null )
                    super.header(RDFPatchConst.PREV, x.asNode());
            }

            RDFPatch patch = getRDFPatch();
            //FmtLog.info(LOG,  "Send patch: id=%s, prev=%s", Id.str(patch.getId()), Id.str(patch.getPrevious()));
            //long newVersion = dLink.append(dsRef, patch);
            //setLocalState(newVersion, patch.getId());

            try {
                append(patch);
            } catch(DeltaBadRequestException ex) {
                FmtLog.warn(LOG, "Failed to commit: %s", ex.getMessage());
                throw ex;
            } finally {
                currentTransactionId = null;
                reset();
                releaseLock();
            }
        }

        @Override
        public void txnAbort() {
            currentTransactionId = null;
            super.txnAbort();
            reset();
            releaseLock();
        }
    }

    /*package*/ void start() {
        checkDeltaConnection();
        // Allow for "async sync" - don't hold up "start".

        if ( TestModeNoAsync ) {
            trySyncIfAuto();
            return;
        }
        boolean withBackgroundSync = (syncPolicy != SyncPolicy.NONE);
        start(withBackgroundSync);
    }

    /*package*/ void start(boolean withBackgroundSync) {
        checkDeltaConnection();
        if ( withBackgroundSync ) {
            // Run (almost) immediately and then every 5 minutes
            scheduledExecutionService.scheduleAtFixedRate(this::oneSyncAttempt, 0, 5*60, TimeUnit.SECONDS);
        }
    }

    /*package*/ void finish() {
        if ( isValid() ) {
            this.logLockMgr.stop();
            this.shutdownSyncExecutorService();
            this.valid = false;
        }
    }

    /** Execute a DeltaConnection sync once. */
    private void oneSyncAttempt() {
        trySyncIfAuto();
    }

    /**
     * No-op end-to-end operation. This operation succeeds or throws an exception.
     * This operation makes one attempt only to perform the ping.
     */
    public void ping() {
        dLink.ping();
    }

    /** Send a patch to log server. */
    public void append(RDFPatch patch) {
        synchronized(localLock) {
            checkDeltaConnection();
            Version ver = dLink.append(datasourceId, patch);
            if ( ! Version.isValid(ver) )
                // Didn't happen.
                return ;
            Version ver0 = state.version();
            if ( ver0.value() >= ver.value() )
                FmtLog.warn(LOG, "[%s] Version did not advance: %d -> %d", datasourceId.toString(), ver0 , ver);
            state.updateState(ver, Id.fromNode(patch.getId()));
        }
    }

    public RDFPatch fetch(Version version) {
        return dLink.fetch(datasourceId, version);
    }

    /** Try to sync ; return true if succeeded, else false */
    public boolean trySync() {
        return attempt(()->sync());
    }

    /** Try to sync by {@link PatchLogInfo} ; return true if succeeded, else false */
    public boolean trySync(PatchLogInfo logInfo) {
        return attempt(()->sync(logInfo));
    }

    /** Sync to a specific log state. */
    public void sync(PatchLogInfo logInfo) {
        checkDeltaConnection();
        syncToVersion(logInfo.getMaxVersion());
    }

    /**
     * Sync if the policy is not NONE, the manual mode.
     * Return true is a sync succeeded, else false.
     * Return false if the SyncPolicy is NONE.
     */
    public boolean trySyncIfAuto() {
        if ( syncPolicy == SyncPolicy.NONE )
            return false;
        return trySync();
    }

    public void sync() {
        try {
            checkDeltaConnection();
            PatchLogInfo logInfo = getPatchLogInfo();
            sync(logInfo);
        } catch (HttpException ex) {
            if ( ex.getStatusCode() == -1 )
                throw new HttpException(HttpSC.SERVICE_UNAVAILABLE_503, HttpSC.getMessage(HttpSC.SERVICE_UNAVAILABLE_503), ex.getMessage());
            throw ex;
        }
    }

    // Attempt an operation and return true/false as to whether it succeeded or not.
    private boolean attempt(Runnable action) {
        try { action.run(); return true ; }
        catch (RuntimeException ex ) { return false ; }
    }

    /**
     * Indicator of whether a sync is in-progress on another thread.
     * This is not guaranteed to be accurate.
     * Synchronization may still block or it may still skip a sync.
     */
    public boolean syncInProgress() {
        return syncInProgress.get();
    }

    /**
     * Sync until some version.
     * sync does not happen if another sync is in-progress.
     * This operation takes the connection lock.
     * Calls may wish to skip sync()
     */
    private void syncToVersion(Version version) {
        if ( ! Version.isValid(version) ) {
            FmtLog.debug(LOG, "Sync: Asked for no patches to sync");
            return;
        }
        if ( syncInProgress() )
            return;
        syncToVersion(version, false);
    }

    /**
     * Sync until some version.
     * This is the work of synchronization.
     * This operation takes the connection lock.
     */
    private void syncToVersion(Version version, boolean allowOverlap) {
        synchronized(localLock) {
            // Inside lock - only one thread.
            if ( !allowOverlap && syncInProgress() )
                return ;
            try {
                syncInProgress.set(true);
                Version localVer = getLocalVersion();
                if ( localVer.isUnset() ) {
                    FmtLog.warn(LOG, "[%s] Local version is UNSET : sync to %s not done", datasourceId, version);
                    return;
                }
                if ( localVer.value() > version.value() )
                    FmtLog.info(LOG, "[%s] Local version ahead of remote : [local=%d, remote=%d]", datasourceId, getLocalVersion(), getRemoteVersionCached());
                if ( localVer.value() >= version.value() )
                    return;
                // localVer is not UNSET so next version to fetch is +1 (INIT is version 0)
                FmtLog.info(LOG, "[%s:%s] Sync start: Versions [%s, %s]", datasourceId, datasourceName, localVer, version);
                // This updates the local state.
                playPatches(localVer, localVer.value()+1, version.value()) ;
                Version localVer2 = getLocalVersion();
                FmtLog.info(LOG, "[%s:%s] Sync finish: Version [%s]", datasourceId, datasourceName, localVer2);
            } finally {
                syncInProgress.set(false);
            }
        }
    }

    /** Play the patches (range is inclusive at both ends); set the new local state on exit. */
    private void playPatches(Version currentVersion, long firstPatchVer, long lastPatchVer) {
        // Inside synchronized of syncToVersion
        Pair<Version, Node> p = play(datasourceId, base, target, dLink, currentVersion, firstPatchVer, lastPatchVer);
        if ( p == null )
            // Didn't make progress for some reason.
            return;
        Version patchLastVersion = p.car();
        Node patchLastIdNode = p.cdr();
        if ( patchLastIdNode == null || patchLastVersion.equals(currentVersion) )
            // No progress.
            return;
        setLocalState(patchLastVersion, patchLastIdNode);
    }

    /** Play patches, return details of the the last successfully applied one */
    private static Pair<Version, Node> play(Id datasourceId, DatasetGraph base, RDFChanges target, DeltaLink dLink,
                                            Version currentVersion,
                                            long minVersion, long maxVersion) {
        // [Delta] replace with a one-shot "get all patches" operation.
        //FmtLog.debug(LOG, "Patch range [%d, %d]", minVersion, maxVersion);

        // Switch off transactions inside of each patch and execute as a single, overall transaction.
        RDFChanges c = new RDFChangesExternalTxn(target);
        if ( false )
            c = DeltaOps.print(c);
        try {
            return Txn.calculateWrite(base, ()->{
                Node patchLastIdNode = null;
                Version patchLastVersion = currentVersion;

                for ( long ver = minVersion ; ver <= maxVersion ; ver++ ) {
                    //FmtLog.debug(LOG, "Play: patch=%s", ver);
                    RDFPatch patch;
                    Version verObj = Version.create(ver);
                    try {
                        patch = dLink.fetch(datasourceId, verObj);
                        if ( patch == null ) {
                            // No patch. Patches have no gaps.
                            // But a storage like S3 is only eventually consistent so stop
                            // now and resync next time.
                            FmtLog.info(LOG, "Play: %s patch=%s : not found", datasourceId, verObj);
                            break;
                        }
                    } catch (DeltaNotFoundException ex) {
                        // Which ever way it is signalled.  This way means "bad datasourceId"
                        FmtLog.info(LOG, "Play: %s patch=%s : not found (no datasource)", datasourceId, verObj);
                        continue;
                    }
                    patch.apply(c);
                    patchLastIdNode = patch.getId();
                    patchLastVersion = verObj;
                }
                return Pair.create(patchLastVersion, patchLastIdNode);
            });
        } catch (Throwable th) {
            FmtLog.warn(LOG, th, "Play: Problem for %s", datasourceId);
            throw th;
        }
    }

    @Override
    public void close() {
        // Send of try-with-resources block.
        // The connection is still usable so don't throw away the
        // Call finish() when a connection is not going to be used again.
    }

    private void shutdownSyncExecutorService() {
    	this.scheduledExecutionService.shutdown();
    	boolean ok = false;
    	try {
    		ok = this.scheduledExecutionService.awaitTermination(30, TimeUnit.SECONDS);
		} catch (InterruptedException ex) {
			LOG.warn("DeltaConnection.scheduledExecutionService.awaitTermination(30, TimeUnit.SECONDS) interrupted", ex);
			return; // skip OK check below
		}
    	if ( ! ok) {
    		LOG.warn("DeltaConnection.scheduledExecutionService.awaitTermination(30, TimeUnit.SECONDS) timed out");
    	}
    }

    public boolean isValid() {
        return valid;
    }

    public DeltaLink getLink() {
        return dLink;
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
                if ( Version.isValid(getRemoteVersionCached()) && info.getMaxVersion().value() < getRemoteVersionCached().value() ) {
                    String dsName = dLink.getDataSourceName(datasourceId);
                    FmtLog.warn(LOG, "[ds:%s %s] Remote version behind local tracking of remote version: [%s, %s]",
                                     datasourceId, dsName, info.getMaxVersion().value(), getRemoteVersionCached().value());
                }
            }
            // Set the local copy whenever we get the remote latest.
            remote.set(info);
        }
        return info;
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
    public Version getRemoteVersionLatest() {
        checkDeltaConnection();
        PatchLogInfo info = getPatchLogInfo();
        if ( info == null )
            return Version.UNSET;
        return info.getMaxVersion();
    }

    /** Return the version of the local data store */
    public Version getLocalVersion() {
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
    private void setLocalState(Version version, Node patchId) {
        setLocalState(version, (patchId == null) ? null : Id.fromNode(patchId));
    }

    /** Update the version of the local data store */
    private void setLocalState(Version version, Id patchId) {
        state.updateState(version, patchId);
    }

    /** Return our local track of the remote version */
    private Version getRemoteVersionCached() {
        //checkDeltaConnection();
        if ( remote.get() == null )
            return Version.UNSET;
        return remote.get().getMaxVersion();
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

    /**
     * The "record changes" version, suppresses empty commits on the RDFChanges.
     * @see RDFChangesSuppressEmpty
     */
    public DatasetGraph getDatasetGraphNoEmpty() {
        checkDeltaConnection();
        return managedNoEmpty;
    }

    /**
     * The "record changes" version, suppresses empty commits on the RDFChanges.
     * @see RDFChangesSuppressEmpty
     */
    public Dataset getDatasetNoEmpty() {
        return managedNoEmptyDataset;
    }

    /** The "without changes" storage */
    public DatasetGraph getStorage() {
        return base;
    }

    public void addListener(DeltaLinkListener listener) {
        dLink.addListener(listener);
    }

    public void removeListener(DeltaLinkListener listener) {
        dLink.removeListener(listener);
    }

    @Override
    public String toString() {
        String str = String.format("DConn %s [local=%d, remote=%d]", datasourceId, getLocalVersion(), getRemoteVersionCached());
        if ( ! valid )
            str = str + " : invalid";
        return str;
    }
}
