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
import static org.seaborne.delta.server.local.patchstores.zk.ZkConst.nPatches;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonValue;
import org.apache.jena.atlas.lib.SetUtils;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.zookeeper.Watcher;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.server.local.*;
import org.seaborne.delta.server.local.patchstores.PatchLogBase;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory; 

public class PatchStoreZk extends PatchStore {
    private final static Logger LOGZK = LoggerFactory.getLogger(PatchStoreZk.class);
    // Use ServiceLoader.
    public static final String PatchStoreZkName = "PatchStoreZk";
    private final CuratorFramework client;
    
    // Schema!
    // https://curator.apache.org/curator-framework/schema.html
    
    /*package*/ PatchStoreZk(CuratorFramework client, PatchStoreProvider psp) { 
        super(psp);
        this.client = client;
    }
    
    public static PatchStore create(CuratorFramework client) {
        return new PatchStoreProviderZk(client).create();
    }

    private static void formatPatchStore(CuratorFramework client) throws Exception {
        zkCreate(client, ZkConst.pRoot);
        zkCreate(client, ZkConst.pLogs);
        zkCreate(client, ZkConst.pStoreLock);
//        zkEnsure(client, ZkConst.pRoot);
//        zkEnsure(client, ZkConst.pStoreLock);
    }
    
    private void init() throws Exception {
        List<DataSourceDescription> x = listDataSources();
        x.forEach(dsd->FmtLog.info(LOGZK, "Log: %s", dsd));
        lastSeen = new HashSet<>(x);
        patchLogWatcher.process(null);
        // Use the ZooKeeper state and don't cache anything here.
        // Small timing hole!
        // But we do not rely on the local list of seen logs.
        // If worried, call scanForLogs()() here. 
    }

    // ---- Watching for logs
    // Watch for log changes.
    private Object watcherLock = new Object();
    private volatile Set<DataSourceDescription> lastSeen = Collections.emptySet(); 
    private Watcher patchLogWatcher = (event)->{
        // Even may be null if called from this process. 
        synchronized(watcherLock) {
            // Reload ASAP.
            watchForLogs();
            // Look at children.
            lastSeen = scanForLogChanges();
        }
    };
    
    private void watchForLogs() {
        zkRun(() -> client.getChildren().usingWatcher(patchLogWatcher).forPath(ZkConst.pLogs) );
    }
    
    /** Do a scan for logs now. */
    private void scanForLogs() {  
        synchronized(watcherLock) {
            scanForLogChanges();
        }
    }
    
    private Set<DataSourceDescription> scanForLogChanges() {
        Set<DataSourceDescription> nowSeen = new HashSet<>(listDataSources());
        // Access once.
        Set<DataSourceDescription> lastSeen$ = lastSeen;
        Set<DataSourceDescription> newLogs = SetUtils.difference(nowSeen, lastSeen$);
        Set<DataSourceDescription> deletedLogs = SetUtils.difference(lastSeen$, nowSeen);
        if ( ! newLogs.isEmpty() )
            FmtLog.info(LOGZK, "New: "+newLogs);
        if ( ! deletedLogs.isEmpty() )
            FmtLog.info(LOGZK, "Deleted: "+deletedLogs);
        return nowSeen;
    }
    // ---- Watching for logs
    
    @Override
    public boolean callInitFromPersistent(LocalServerConfig config) {
        return true;
    }

    @Override
    public List<DataSource> initFromPersistent(LocalServerConfig config) {
        boolean isEmpty = zkCalc(()->client.checkExists().forPath(ZkConst.pRoot)==null);
        try {
            if ( isEmpty ) {
                LOGZK.info("No PatchStore - format new one"); 
                formatPatchStore(client);
            } else
                init();
        }
        catch (Exception ex) {
            LOGZK.error("Failed to initialize from the persistent state: "+ex.getMessage(), ex);
            return Collections.emptyList();
        }
        
        List<DataSource> dataSources = listDataSources().stream().map(dsd->DataSource.connect(dsd, this, null)).collect(Collectors.toList());
        return dataSources;
    }

    @Override
    public void addDataSource(DataSource ds, JsonObject sourceObj, Path dataSourceArea) {
        create(ds.getDescription(), dataSourceArea);
    }

    @Override
    public List<DataSourceDescription> listDataSources() {
        List<DataSourceDescription> descriptions = new ArrayList<DataSourceDescription>();
        
        List<String> logNames = Zk.zkSubNodes(client, ZkConst.pLogs);
        if ( logNames.isEmpty() )
            LOGZK.info("<No logs>");
        else {
            // XXX logNames.stream()
            logNames.forEach(name->{
                String logPath = ZKPaths.makePath(ZkConst.pLogs, name);
                String logDsd = ZKPaths.makePath(ZkConst.pLogs, name, ZkConst.nDsd);
                JsonObject obj = zkFetchJson(client, logDsd);
                DataSourceDescription dsd = DataSourceDescription.fromJson(obj);
                descriptions.add(dsd);
            });
        }
        return descriptions;
    }
    
    @Override
    protected PatchLog create(DataSourceDescription dsd, Path dsPath) {
        String dsName = dsd.getName();
        if ( ! validateName(dsName) ) {
            String msg = String.format("Log name '%s' does not match regex '%s'", dsName, LogNameRegex);
            Log.warn(LOGZK, msg);
            throw new DeltaBadRequestException(msg);
        } 
        // format
        
        String logPath = zkPath(ZkConst.pLogs, dsName);
        if ( ! zkExists(client, logPath) ) {
            zkLock(client, ZkConst.pStoreLock, ()->{
                formatLog(dsd, logPath);
            });
        }
        Zk.listNodes(client, logPath);
        // no state
        PatchLogIndex store = new PatchLogIndexZk(client, zkPath(logPath, ZkConst.nState), zkPath(logPath, ZkConst.nVersions));
        PatchStorage storage = new PatchStorageZk(client, zkPath(logPath, ZkConst.nPatches));
        PatchLog patchLog = new PatchLogBase(dsd, store, storage, this) ;
        return patchLog;
    }
    
    @Override
    protected void delete(PatchLog patchLog) {
        String dsName = patchLog.getDescription().getName();
        String logPath = zkPath(ZkConst.pLogs, dsName);
        if ( zkExists(client, logPath) )
            zkDelete(client, logPath);
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
     *   /delta/logs/NAME/archive
     * </pre>
     */
    private void formatLog(DataSourceDescription dsd, String logPath) {
        zkCreate(client, logPath);
        zkCreate(client, zkPath(logPath, nLock));
        zkCreate(client, zkPath(logPath, nPatches));
        //zkCreate(client, zkPath(logPath, nVersionsSeq), CreateMode.PERSISTENT_SEQUENTIAL);

        String dsdPath = zkPath(logPath, nDsd);

        // DSD
        JsonObject dsdJson = dsd.asJson();
        byte[] b1 = jsonBytes(dsdJson);
        zkCreateSet(client, dsdPath, b1);

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
}
