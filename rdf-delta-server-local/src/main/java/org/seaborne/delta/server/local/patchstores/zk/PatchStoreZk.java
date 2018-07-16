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

package org.seaborne.delta.server.local.patchstores.zk;

import static org.seaborne.delta.server.local.patchstores.zk.Zk.*;
import static org.seaborne.delta.server.local.patchstores.zk.ZkConst.nDsd;
import static org.seaborne.delta.server.local.patchstores.zk.ZkConst.nLock;
import static org.seaborne.delta.server.local.patchstores.zk.ZkConst.nState;
import static org.seaborne.delta.server.local.patchstores.zk.ZkConst.nVersions;

import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonValue;
import org.apache.jena.atlas.lib.ListUtils;
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
import org.seaborne.delta.server.local.patchstores.zk.Zk.ZkRunnable;
import org.seaborne.delta.server.local.patchstores.zk.Zk.ZkSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory; 

public class PatchStoreZk extends PatchStore {
    private final static Logger LOGZK = LoggerFactory.getLogger(PatchStoreZk.class);
    // Use ServiceLoader.
    public static final String PatchStoreZkName = "PatchStoreZk";
    // Schema!
    // https://curator.apache.org/curator-framework/schema.html
    
    // ---- Watching for logs
    private final Object storeLock = new Object();
    
    private static int counter = 0; 
    private final CuratorFramework client;
    private final int instance;
    
    // Schema!
    // https://curator.apache.org/curator-framework/schema.html
    
    // Our view of all the patch logs in the store.
    private Map<String, PatchLogZk> patchLogs = new ConcurrentHashMap<>();

    /*package*/ PatchStoreZk(PatchStoreProvider psp) {
        this(null, psp);
    }
    
    /*package*/ PatchStoreZk(CuratorFramework client, PatchStoreProvider psp) { 
        super(psp);
        // Null "client" means new one each for patch store.
        this.instance = ++counter; 
        this.client = client;
    }

    public static PatchStore create(int x, LocalServerConfig config) {
        return new PatchStoreProviderZk(null).create(config);
    }

    private static void formatPatchStore(CuratorFramework client) throws Exception {
        if ( ! zkExists(client, ZkConst.pRoot) )
            zkCreate(client, ZkConst.pRoot);
        zkEnsure(client, ZkConst.pStoreLock);
        // It does not matter if this multiple operations - it happens rarely. 
        Zk.zkLock(client, ZkConst.pStoreLock, ()->{
            if ( ! zkExists(client, ZkConst.pLogs) )
                zkCreate(client, ZkConst.pLogs);
            if ( ! zkExists(client, ZkConst.pActiveLogs) )
                zkCreate(client, ZkConst.pActiveLogs);
        });
    }
    
    private void init() throws Exception {
        List<DataSourceDescription> x = listDataSourcesZk();
        Set<String> names = getWatchLogs();
        updateLogChanges(names);
    }

    private Watcher patchLogWatcher = (event)->{
        synchronized(storeLock) {
            // get names.
            Set<String> names = getWatchLogs();
            updateLogChanges(names);
        }
    };
    
    private Set<String> getWatchLogs() {
        List<String> logNames = zkCalc(() -> client.getChildren().usingWatcher(patchLogWatcher).forPath(ZkConst.pActiveLogs) );
        return new HashSet<String>(logNames);
    }
    
//    private Set<DataSourceDescription> scanForLogChanges() {
//        Set<DataSourceDescription> nowSeen = new HashSet<>(listDataSources());
        
    private void updateLogChanges(Set<String> names) {
        Set<String> lastSeenLocal = patchLogs.keySet(); 
        Set<String> newLogs = SetUtils.difference(names, lastSeenLocal);
        Set<String> deletedLogs = SetUtils.difference(lastSeenLocal, names);

        if ( newLogs.isEmpty() && deletedLogs.isEmpty() )
            return;
        FmtLog.debug(LOGZK, "[%d] New=%s : Deleted=%s", instance, newLogs, deletedLogs);
        
        newLogs.forEach(name->{
            // Read DSD.
            String newLogPath = zkPath(ZkConst.pActiveLogs, name);
            JsonObject obj = Zk.zkFetchJson(client, newLogPath);
            if ( obj == null ) {
                FmtLog.info(LOGZK, "[%d] New=%s : DSD not found", instance, name);
                return; 
            }
            // Create local.
            DataSourceDescription dsd = DataSourceDescription.fromJson(obj);
            createPatchLog(dsd);
        });

        deletedLogs.forEach(name->{
            // Find the dsd via local cache info only.
            PatchLogZk patchLog = patchLogs.get(name);
            if ( patchLog == null )
                return;
            releasePatchLog(patchLog.getLogId());
        });
    }
    // ---- Watching for logs
    
    @Override
    protected List<DataSourceDescription> initialize(LocalServerConfig config) {
        if ( client == null )
            return Collections.emptyList();
        
        boolean isEmpty = zkCalc(()->client.checkExists().forPath(ZkConst.pRoot)==null);
        try {
            if ( isEmpty ) {
                FmtLog.debug(LOGZK, "[%d] No PatchStore - format new one", instance); 
                formatPatchStore(client);
            } else
                init();
            getWatchLogs();
        }
        catch (Exception ex) {
            LOGZK.error("Failed to initialize from the persistent state: "+ex.getMessage(), ex);
            return Collections.emptyList();
        }
        return listDataSources();
    }

    @Override
    public List<DataSourceDescription> listDataSources() {
        FmtLog.debug(LOGZK, "[%d] listDataSources", instance);
        return ListUtils.toList(
            patchLogs.values().stream().map(log->log.getDescription())
            );
        
        //return listDataSourcesZk();
    }
    
    // Guarantee to look in zookeeper, no local caching.
    private List<DataSourceDescription> listDataSourcesZk() {
        return listDataSourcesZkPath(ZkConst.pActiveLogs);
    }
    
    // Guarantee to look in the logs area, not the active logs, with no local caching.
    private List<DataSourceDescription> listDataSourcesZkAll() {
        return listDataSourcesZkPath(ZkConst.pLogs);
    }

    // Get all logs named by the path zNode.
    private List<DataSourceDescription> listDataSourcesZkPath(String logsPath) {
        List<DataSourceDescription> descriptions = new ArrayList<DataSourceDescription>();
        List<String> logNames = Zk.zkSubNodes(client, logsPath);
        for ( String name: logNames) {
            String logDsd = zkPath(ZkConst.pLogs, name, ZkConst.nDsd);
            JsonObject obj = zkFetchJson(client, logDsd);
            if ( obj == null ) {
                FmtLog.info(LOGZK, "[%d] listDataSourcesZkPath: %s: no DSD", instance, name);
                continue;
            }
            DataSourceDescription dsd = DataSourceDescription.fromJson(obj);
            descriptions.add(dsd);
        };
        return descriptions;
    }


    // Compare and contrast.
    private List<DataSourceDescription> listDataSourcesZkPath_alt(String logsPath) {
        List<String> logNames = Zk.zkSubNodes(client, logsPath);
        Stream<DataSourceDescription> descriptions =
            logNames.stream()
                .map(name->{
                    FmtLog.info(LOGZK, "[%d] listDataSources: %s", instance, name);
                    String logDsd = ZKPaths.makePath(ZkConst.pLogs, name, ZkConst.nDsd);
                    JsonObject obj = zkFetchJson(client, logDsd);
                    if ( obj == null ) {
                        FmtLog.info(LOGZK, "[%d] listDataSourcesZkPath: %s: no DSD", instance, name);
                        return null;
                    }
                    DataSourceDescription dsd = DataSourceDescription.fromJson(obj);
                    return dsd;
                })
                .filter(Objects::nonNull)
                ;
        return ListUtils.toList(descriptions);
    }
    
    @Override
    protected PatchLog create(DataSourceDescription dsd) {
        synchronized(storeLock) {
            return create$(dsd);
        }
    }
    
    private PatchLog create$(DataSourceDescription dsd) {
        // This is called to create a log when the application asks but also when the
        // store watcher sees a new log elsewhere appearing.
        FmtLog.debug(LOGZK,  "[%d] create patch log '%s'", instance, dsd.getName());
        
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

        String logPath = zkPath(ZkConst.pLogs, dsName);
        if ( ! zkExists(client, logPath) ) {
            FmtLog.debug(LOGZK, "[%d] Does not exist: format", instance);
            clusterLock(()->{
                // Someone else got in.
                if ( zkExists(client, logPath) ) {
                    FmtLog.debug(LOGZK, "[%d] exists: skip format", instance);
                    return;
                }
                formatPatchLog(client, dsd, logPath);
                
                // The watcher watch path ZkConst.pActiveLogs so adding a zNode 
                // signals the log exists and is ready. It will trigger the watchers.
                // We are still inside the clusterLock and also the storeLock.
                // Our own watcher will wait until we leave storeLock, and then find
                // that "patchLogs.containsKey(dsd.getName())"
                String zkActiveLog = zkPath(ZkConst.pActiveLogs, dsName);
                JsonObject dsdJson = dsd.asJson();
                zkCreateSetJson(client, zkActiveLog, dsdJson);
            }, null);
            FmtLog.debug(LOGZK, "[%d] format done", instance);
        } 

        // Local storeLock still held.
        // create local the local object.
        PatchLogZk patchLog = new PatchLogZk(dsd, instance, logPath, client, this);
        patchLogs.put(dsName, patchLog);
        return patchLog;
    }
    
    @Override
    protected void delete(PatchLog patchLog) {
        clusterLock(()->{
            String dsName = patchLog.getDescription().getName();
            PatchLog patchLog2 = patchLogs.remove(dsName);
            if ( patchLog2 == null )
                return; 
            // Remove from activeLogs, leave in the "logs" area.
            // This triggers watchers.
            String zkActiveLog = zkPath(ZkConst.pActiveLogs, dsName);
            if ( zkExists(client, zkActiveLog) )
                zkDelete(client, zkActiveLog);
            String logPath = zkPath(ZkConst.pLogs, dsName);
            // Clear up.
            zkDelete(client, logPath);
        }, null);
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
    private static void formatPatchLog(CuratorFramework client, DataSourceDescription dsd, String logPath) {
        // Called inside cluster lock and the patch store Java lock.
        
        // The JSON objects.
        JsonObject dsdJson = dsd.asJson();
        JsonObject initialState = PatchLogIndexZk.initialStateJson();

        zkCreate(client, logPath);
        
        // PatchStorageZk (created there as well)
        //    zkCreate(client, zkPath(logPath, nPatches));

        // PatchLogIndexZk uses:
        String dsdPath        = zkPath(logPath, nDsd);
        String statePath      = zkPath(logPath, nState);
        String versionsPath   = zkPath(logPath, nVersions);
        String lockPath       = zkPath(logPath, nLock);

        // Paths.
        zkCreateSetJson(client, dsdPath, dsdJson);
        zkCreateSetJson(client, statePath, initialState);
        zkCreate(client, versionsPath);
        zkCreate(client, lockPath);
    }
    
    private static byte[] jsonBytes(JsonValue json) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(8*1024);
        JSON.write(out, json);
        return out.toByteArray();
    }

    private static String LogNameRegex = "[A-Za-z0-9][-A-Za-z0-9_.]*[A-Za-z0-9_$]?";
    private static Pattern LogNamePattern = Pattern.compile(LogNameRegex);
    private static boolean validateName(String dsName) {
        return LogNamePattern.matcher(dsName).matches();
    }
    
    // Execute an action with a local lock and store-wide lock.
    private void clusterLock(ZkRunnable action, Consumer<Exception> onThrow) {
        synchronized(storeLock) {
            Zk.zkLock(client, ZkConst.pStoreLock, ()->{
                try {
                    action.run();
                } catch(Exception ex) {
                    if ( onThrow != null )
                        onThrow.accept(ex);
                }
            });
        }
    }
    
    // Execute an action with a local lock and store-wide lock.  
    private <X> X clusterLock(ZkSupplier<X> action, Consumer<Exception> onThrow) {
        synchronized(storeLock) {
            X x = 
                Zk.zkLockRtn(client, ZkConst.pStoreLock, ()->{
                    try {
                        return action.run();
                    } catch(Exception ex) {
                        if ( onThrow != null )
                            onThrow.accept(ex);
                        return null;        
                    }
                });
            return x;

        }
    }

}
