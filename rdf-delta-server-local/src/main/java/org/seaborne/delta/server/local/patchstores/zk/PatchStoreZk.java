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

package org.seaborne.delta.server.local.patchstores.zk;

import static org.seaborne.delta.server.local.patchstores.zk.ZkConst.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.curator.utils.ZKPaths;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.lib.SetUtils;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.zookeeper.Watcher;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchLog;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchStoreProvider;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.delta.zk.UncheckedZkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatchStoreZk extends PatchStore {
    private final static Logger LOGZK = LoggerFactory.getLogger(PatchStoreZk.class);
    // Schema!
    // https://curator.apache.org/curator-framework/schema.html

    // ---- Watching for logs
    private final Object storeLock = new Object();

    private static final AtomicInteger counter = new AtomicInteger(1);
    private final UncheckedZkConnection client;
    private final String instance;

    // Our view of all the patch logs in the store.
    private final Map<String, PatchLog> patchLogs = new ConcurrentHashMap<>();

    protected PatchStoreZk(UncheckedZkConnection client, PatchStoreProvider psp) {
        super(psp);
        this.instance = "zk-"+(counter.getAndIncrement());
        this.client = client;
    }

    public UncheckedZkConnection getClient() { return this.client; }
    public String getInstance() { return instance; }

    private void formatPatchStore() {
        this.client.ensurePathExists(ZkConst.pRoot);
        this.client.ensurePathExists(ZkConst.pStoreLock);
        // It does not matter if this holds up multiple log stores when they all run - it happens rarely.
        this.client.runWithLock(ZkConst.pStoreLock, ()-> {
            if ( ! this.client.pathExists(ZkConst.pLogs) )
                this.client.createZNode(ZkConst.pLogs);
            if ( ! this.client.pathExists(ZkConst.pActiveLogs) )
                this.client.createZNode(ZkConst.pActiveLogs);
        });
    }

    private void initPatchStoreZk() {
        // Also sets watcher.
        List<String> names = getWatchLogs();
        updateLogChanges(names);
    }

    public static final boolean actionsInWatcher = true;
    private final Watcher patchLogWatcher = (event)->{
        synchronized(storeLock) {
            List<String> names = getWatchLogs();
            if ( actionsInWatcher )
                updateLogChanges(names);
        }
    };

    private void updateLogChanges(List<String> namesList) {
        if ( namesList == null ) {
            //FmtLog.info(LOGZK, "[%s] updateLogChanges[%s] -> null", instance, label);
            return;
        }
        Set<String> x = new HashSet<>(namesList);
        //FmtLog.info(LOGZK, "[%s] updateLogChanges[%s] -> %s", instance, label, x);
        updateLogChanges(x);
    }

    // Update based on a set of active logs.
    private void updateLogChanges(Set<String> names) {
        // Might be better to queue all requests, and have one worker do the updates then
        // release all waiting threads.  However, logs don't get created and deleted very often
        // so the work here is usually calculating empty sets.

        // Overlapping actions, and any inconsistency they generate, don't matter too much because
        // as the system settles down, the correct view will be in-place.

        Set<String> lastSeenLocal = patchLogs.keySet();
        Set<String> newLogs = SetUtils.difference(names, lastSeenLocal);
        Set<String> deletedLogs = SetUtils.difference(lastSeenLocal, names);

        if ( newLogs.isEmpty() && deletedLogs.isEmpty() )
            return;
        FmtLog.debug(LOGZK, "[%s] New=%s : Deleted=%s", instance, newLogs, deletedLogs);
        //FmtLog.info(LOGZK, "[%s] New=%s : Deleted=%s", instance, newLogs, deletedLogs);

        newLogs.forEach(name->{
            // Read DSD.
            String newLogPath = ZKPaths.makePath(ZkConst.pActiveLogs, name, new String[]{});
            JsonObject obj = this.client.fetchJson(newLogPath);
            if ( obj == null ) {
                FmtLog.info(LOGZK, "[%s] New=%s : DSD not found", instance, name);
                return;
            }
            // Create local.
            DataSourceDescription dsd = DataSourceDescription.fromJson(obj);
            createPatchLog(dsd);
        });

        deletedLogs.forEach(name->{
            // Find the dsd via local cache info only.
            PatchLog patchLog = patchLogs.get(name);
            if ( patchLog == null )
                return;
            notifyDeletionPatchLog(patchLog.getLogId());
        });
        FmtLog.debug(LOGZK, "[%s] Done", instance);
        //FmtLog.info(LOGZK, "[%s] Done", instance);
    }
    // ---- Watching for logs

    private List<String> getWatchLogs() {
        // This can happen too early when /delta/activeLogs is being setup.
        // Just ignore that and sync later.
        return this.client.fetchChildren(patchLogWatcher, ZkConst.pActiveLogs);
    }

    private List<String> activeLogs() {
        // No "usingWatcher"
        return this.client.fetchChildren(ZkConst.pActiveLogs);
    }

    @Override
    protected void sync() {
        //FmtLog.debug(LOGZK, "[%s] Store sync", instance);
        try {
            List<String> logNames = activeLogs();
            //FmtLog.debug(LOGZK, "[%s] Store sync : %s", instance, logNames);
            updateLogChanges(logNames);
        } catch (Exception ex) {
            FmtLog.warn(LOGZK, "Exception in sync: "+ex.getClass().getSimpleName()+" "+ex.getMessage());
        }
    }

    @Override
    protected void initialize(LocalServerConfig config) {
        if ( client == null ) {
            FmtLog.warn(LOGZK, "[%s] No Curator client", instance);
            return;
        }

        boolean isEmpty = !this.client.pathExists(ZkConst.pRoot);
        try {
            if ( isEmpty ) {
                FmtLog.info(LOGZK, "[%s] Format new PatchStoreZk", instance);
                formatPatchStore();
            } else
                initPatchStoreZk();
            getWatchLogs();
        }
        catch (Exception ex) {
            LOGZK.error("Failed to initialize from the persistent state: "+ex.getMessage(), ex);
        }
    }

    @Override
    protected List<DataSourceDescription> initialDataSources() {
        // This is sync'ed to the Zk state.
        return listDataSourcesZk();
    }

    @Override
    protected void shutdownSub() {
        if ( this.client != null ) {
            this.client.close();
        }
    }

    @Override
    public List<DataSourceDescription> listDataSources() {
        FmtLog.debug(LOGZK, "[%d] listDataSources", instance);
        sync();
        return super.listDataSources();
    }

    // Guaranteed to look in zookeeper, no local caching.
    private List<DataSourceDescription> listDataSourcesZk() {
        return listDataSourcesZkPath();
    }

    // Get all logs named by the path zNode.
    private List<DataSourceDescription> listDataSourcesZkPath() {
        List<DataSourceDescription> descriptions = new ArrayList<>();
        List<String> logNames = this.client.fetchChildren(pActiveLogs);
        if ( logNames == null )
            return Collections.emptyList();
        for ( String name: logNames) {
            String logDsd = ZKPaths.makePath(ZkConst.pLogs, name, ZkConst.nDsd);
            JsonObject obj = this.client.fetchJson(logDsd);
            if ( obj == null ) {
                FmtLog.info(LOGZK, "[%d] listDataSourcesZkPath: %s: no DSD", instance, name);
                continue;
            }
            DataSourceDescription dsd = DataSourceDescription.fromJson(obj);
            descriptions.add(dsd);
        }
        return descriptions;
    }

    @Override
    protected PatchLog newPatchLog(DataSourceDescription dsd) {
        synchronized(storeLock) {
            return create$(dsd);
        }
    }

    private PatchLog create$(DataSourceDescription dsd) {
        // This is called to create a log when the application asks but also when the
        // store watcher sees a new log elsewhere appearing.
        //
        // If the log does not exists, it creates it in Zookeeper.
        //
        // It implements "new DataSourceDescription"

        // XXX Split the roles, or add a flag.

        if ( patchLogs.containsKey(dsd.getName()) ) {
            FmtLog.debug(LOGZK, "[%d] Found to exist", instance);
            return patchLogs.get(dsd.getName());
        }

        String dsName = dsd.getName();
        if ( ! validateName(dsName) ) {
            String msg = String.format("Log name '%s' does not match regex '%s'", dsName, LogNameRegex);
            Log.warn(LOGZK, msg);
            throw new DeltaBadRequestException(msg);
        }
        // format

        String logPath = ZKPaths.makePath(ZkConst.pLogs, dsName, new String[]{});
        if ( ! this.client.pathExists(logPath) ) {
            FmtLog.debug(LOGZK, "[%d] Does not exist: format", instance);
            clusterLock(()->{

                // Someone else got in.
                if ( this.client.pathExists(logPath) ) {
                    FmtLog.debug(LOGZK, "[%d] exists: skip format", instance);
                    return;
                }
                formatPatchLog(dsd, logPath);

                // The watcher watch path ZkConst.pActiveLogs so adding a zNode
                // signals the log exists and is ready. It will trigger the watchers.
                // We are still inside the clusterLock and also the storeLock.
                // Our own watcher will wait until we leave storeLock, and then find
                // that "patchLogs.containsKey(dsd.getName())"
                String zkActiveLog = ZKPaths.makePath(ZkConst.pActiveLogs, dsName, new String[]{});
                JsonObject dsdJson = dsd.asJson();
                this.client.createAndSetZNode(zkActiveLog, dsdJson);
                //FmtLog.debug(LOGZK, "[%d] format done", instance);
            });
        }
        // Local storeLock still held.
        // create the local object.
        PatchLog patchLog = newPatchLogFromIndexAndStorage(dsd);
        patchLogs.put(dsName, patchLog);
        return patchLog;
    }

    @Override
    protected PatchLogIndex newPatchLogIndex(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration) {
        PatchStoreZk patchStoreZk = (PatchStoreZk)patchStore;
        String logPath = ZKPaths.makePath(ZkConst.pLogs, dsd.getName(), new String[]{});
        return new PatchLogIndexZk(patchStoreZk.getClient(), patchStoreZk.getInstance(), dsd, logPath);
    }

    @Override
    protected PatchStorage newPatchStorage(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration) {
        PatchStoreZk patchStoreZk = (PatchStoreZk)patchStore;
        String logPath = ZKPaths.makePath(ZkConst.pLogs, dsd.getName(), new String[]{});
        return new PatchStorageZk(patchStoreZk.getClient(), patchStoreZk.getInstance(), logPath);
    }

    @Override
    protected void delete(PatchLog patchLog) {
        clusterLock(()->{
            String dsName = patchLog.getDescription().getName();
            PatchLog patchLog2 = patchLogs.remove(dsName);
            if ( patchLog2 == null )
                // Already gone.
                return;
            FmtLog.info(LOGZK,  "[%s] delete patch log '%s'", instance, dsName);
            // Remove from activeLogs, leave in the "logs" area.
            // This triggers watchers.
            String zkActiveLog = ZKPaths.makePath(ZkConst.pActiveLogs, dsName, new String[]{});
            if ( this.client.pathExists(zkActiveLog) ) {
                this.client.deleteZNodeAndChildren(zkActiveLog);
            }
            String logPath = ZKPaths.makePath(ZkConst.pLogs, dsName, new String[]{});
            // Clear up.
            this.client.deleteZNodeAndChildren(logPath);
        });
    }

    /**
     * Format an area for a new patch log. The log area is expected not to exist
     * initially.
     * <pre>
     *   /delta/logs/NAME/dsd        -- PatchLogIndexZk : JSON
     *   /delta/logs/NAME/state      -- JSON, initially VERSION_INIT.
     *   /delta/logs/NAME/versions/  -- The versions
     *   /delta/logs/NAME/lock       -- The patch log lock.
     * </pre>
     * When formatted,
     * <pre>
     *   /delta/activeLogs/NAME
     * </pre>
     * is created.
     */
    private void formatPatchLog(DataSourceDescription dsd, String logPath) {
        // Called inside cluster lock and the patch store Java lock.
        FmtLog.info(LOGZK,  "[%s] create patch log '%s'", instance, dsd.getName());
        // The JSON objects.
        JsonObject dsdJson = dsd.asJson();
        JsonObject initialState = PatchLogIndexZk.initialStateJson();

        this.client.createZNode(logPath);

        // PatchStorageZk (created there as well)
        //    zkCreate(client, zkPath(logPath, nPatches));

        // PatchLogIndexZk uses:
        String dsdPath        = ZKPaths.makePath(logPath, nDsd, new String[]{});
        String statePath      = ZKPaths.makePath(logPath, nState, new String[]{});
        String versionsPath   = ZKPaths.makePath(logPath, nVersions, new String[]{});
        String headersPath    = ZKPaths.makePath(logPath, nHeaders, new String[]{});
        String lockPath       = ZKPaths.makePath(logPath, nLock, new String[]{});
        String lockStatePath  = ZKPaths.makePath(logPath, nLockState, new String[]{});

        // Paths.
        this.client.createAndSetZNode(dsdPath, dsdJson);
        this.client.createAndSetZNode(statePath, initialState);
        this.client.createZNode(versionsPath);
        this.client.createZNode(headersPath);
        this.client.createZNode(lockPath);
        this.client.createZNode(lockStatePath);
    }

    private static final String LogNameRegex = "[A-Za-z0-9][-A-Za-z0-9_.]*[A-Za-z0-9_$]?";
    private static final Pattern LogNamePattern = Pattern.compile(LogNameRegex);
    private static boolean validateName(String dsName) {
        return LogNamePattern.matcher(dsName).matches();
    }

    // Execute an action with a local lock and store-wide lock.
    private void clusterLock(final Runnable action) {
        synchronized(storeLock) {
            this.client.runWithLock(ZkConst.pStoreLock, ()->{
                try {
                    action.run();
                } catch(Exception ignore) { }
            });
        }
    }
}
