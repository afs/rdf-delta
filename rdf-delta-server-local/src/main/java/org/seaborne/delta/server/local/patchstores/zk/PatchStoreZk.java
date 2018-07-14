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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory; 

public class PatchStoreZk extends PatchStore {
    private final static Logger LOGZK = LoggerFactory.getLogger(PatchStoreZk.class);
    // Use ServiceLoader.
    public static final String PatchStoreZkName = "PatchStoreZk";
    // Schema!
    // https://curator.apache.org/curator-framework/schema.html
    
    // ---- Watching for logs
    // Watch for log changes.
    // XXX Cluster lock.
    private Object storeLock = new Object();
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

    private void formatPatchStore() throws Exception {
        zkEnsure(client, ZkConst.pStoreLock);
        // It does not matter if this multiple operations - it happens rarely. 
        Zk.zkLock(client, ZkConst.pStoreLock, ()->{
            if ( ! zkExists(client, ZkConst.pRoot) )
                zkCreate(client, ZkConst.pRoot);
            if ( ! zkExists(client, ZkConst.pLogs) )
                zkCreate(client, ZkConst.pLogs);
        });
    }
    
    private void init() throws Exception {
        List<DataSourceDescription> x = listDataSourcesZk();
//        if ( x.isEmpty() )
//            LOGZK.info("<No logs>");
//        else
//            x.forEach(dsd->FmtLog.info(LOGZK, "Log: %s", dsd));
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
        List<String> logNames = zkCalc(() -> client.getChildren().usingWatcher(patchLogWatcher).forPath(ZkConst.pLogs) );
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
        //FmtLog.info(LOGZK, "** [%d] Store watcher ** now: %s, last: %s", instance, names, lastSeenLocal);
        FmtLog.info(LOGZK, "[%d] New=%s : Deleted=%s", instance, newLogs, deletedLogs);
        
        List<DataSourceDescription> sources = listDataSources();
        Map<String, DataSourceDescription> map = new HashMap<>();
        // XXX Better
        sources.forEach(source->map.put(source.getName(), source)); 
        
        newLogs.forEach(name->{
            DataSourceDescription dsd = map.get(name);
            // Deadlock? Cluster lock?
            createPatchLog(dsd);
        });

        deletedLogs.forEach(name->{
            DataSourceDescription dsd = map.get(name);
            releasePatchLog(dsd.getId());
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
                LOGZK.info("No PatchStore - format new one"); 
                formatPatchStore();
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
        FmtLog.info(LOGZK, "[%d] listDataSources", instance);
        return listDataSourcesZk();
    }
    
    // Guarantee to look in zookeeper, no local caching.
    private List<DataSourceDescription> listDataSourcesZk() {
        List<DataSourceDescription> descriptions = new ArrayList<DataSourceDescription>();
        List<String> logNames = Zk.zkSubNodes(client, ZkConst.pLogs);
        for ( String name: logNames) {
            //String logPath = ZKPaths.makePath(ZkConst.pLogs, name);
            String logDsd = ZKPaths.makePath(ZkConst.pLogs, name, ZkConst.nDsd);
            JsonObject obj = zkFetchJson(client, logDsd);
            if ( obj == null )
                // Timing! Name (/delta/logs/NAME/) has appeared, not the DSD yet.
                // (Otherwise we need to add a lock)
                continue;
            DataSourceDescription dsd = DataSourceDescription.fromJson(obj);
            descriptions.add(dsd);
        };
        return descriptions;
    }
    
    //@Override
    // Compare and contrast.
    public List<DataSourceDescription> listDataSources1() {
        FmtLog.info(LOGZK, "[%d] listDataSources", instance);
        List<String> logNames = Zk.zkSubNodes(client, ZkConst.pLogs);
        Stream<DataSourceDescription> descriptions =
            logNames.stream()
                .map(name->{
                    FmtLog.info(LOGZK, "[%d] listDataSources: %s", instance, name);
                    String logPath = ZKPaths.makePath(ZkConst.pLogs, name);
                    String logDsd = ZKPaths.makePath(ZkConst.pLogs, name, ZkConst.nDsd);
                    JsonObject obj = zkFetchJson(client, logDsd);
                    if ( obj == null )
                        // Timing! Name (/delta/logs/NAME/) has appeared, not the DSD yet.
                        return null;
                    DataSourceDescription dsd = DataSourceDescription.fromJson(obj);
                    return dsd;
                })
                .filter(Objects::nonNull)
                ;
        return ListUtils.toList(descriptions);
    }
    
    @Override
    protected PatchLog create(DataSourceDescription dsd) {
        
        return clusterLock(()->{
            // This is called to create a log when the application asks but also when the store watcher sees 
            // a new log elsewhere appearing leading to potentially overlapping calls when the watcher triggers.
            // Avoid by checking patchLogs inside the lock.
            FmtLog.info(LOGZK,  "[%d] create patch log '%s'", instance, dsd.getName());

            if ( patchLogs.containsKey(dsd.getName()) )
                return patchLogs.get(dsd.getName()); 

            String dsName = dsd.getName();
            if ( ! validateName(dsName) ) {
                String msg = String.format("Log name '%s' does not match regex '%s'", dsName, LogNameRegex);
                Log.warn(LOGZK, msg);
                throw new DeltaBadRequestException(msg);
            } 
            // format

            String logPath = zkPath(ZkConst.pLogs, dsName);
            if ( ! zkExists(client, logPath) ) {
                LOGZK.info("Does not exist: format");
                formatLog(dsd, logPath);
            }
            PatchLogZk patchLog = new PatchLogZk(dsd, instance, logPath, client, this);
            patchLogs.put(dsName, patchLog);
            return patchLog;
        }, 
        (ex)->ex.printStackTrace()); // XXX
    }
    
    @Override
    protected void delete(PatchLog patchLog) {
        synchronized(storeLock) {
            String dsName = patchLog.getDescription().getName();
            PatchLog patchLog2 = patchLogs.remove(dsName);
            if ( patchLog2 == null )
                return; 
            String logPath = zkPath(ZkConst.pLogs, dsName);
            if ( zkExists(client, logPath) )
                zkDelete(client, logPath);
            }
    }

    /**
     * Format an area for a new patch log. The log area is expected not to exist
     * initially.
     * 
     * <pre>
     *   /delta/logs/NAME/dsd        -- JSON
     *   /delta/logs/NAME/lock
     *   /delta/logs/NAME/state      -- JSON 
     *   /delta/logs/NAME/patches-   -- Sequence node
     *   /delta/logs/NAME/versions
     * </pre>
     */
    private void formatLog(DataSourceDescription dsd, String logPath) {
        // DSD
        JsonObject dsdJson = dsd.asJson();
        byte[] bytesDsd = jsonBytes(dsdJson);

        zkCreate(client, logPath);
        zkCreate(client, zkPath(logPath, nLock));
        // Set in PatchStorageZk
        //    zkCreate(client, zkPath(logPath, nPatches));
        // Set in PatchLogIndexZk
        //    zkCreate(client, zkPath(logPath, nVersions));
        //    zkCreate(client, zkPath(logPath, nVersionsSeq), CreateMode.PERSISTENT_SEQUENTIAL);

        String dsdPath = zkPath(logPath, nDsd);
        zkCreateSet(client, dsdPath, bytesDsd);
        // Don't write initial state - handled in PatchLogIndexZk.
    }
    
    private byte[] jsonBytes(JsonValue json) {
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
                    onThrow.accept(ex);
                }
            });
        }
    }
    
    // Execute an action with a local lock and store-wide lock.  
    private <X> X clusterLock(ZkSupplier<X> action, Consumer<Exception> onThrow) {
        synchronized(storeLock) {
            return 
                Zk.zkLockRtn(client, ZkConst.pStoreLock, ()->{
                    try {
                        return action.run();
                    } catch(Exception ex) {
                        onThrow.accept(ex);
                        return null;        
                    }
                });

        }
    }

}
