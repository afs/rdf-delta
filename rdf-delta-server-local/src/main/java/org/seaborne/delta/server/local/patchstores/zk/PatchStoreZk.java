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
import java.util.regex.Pattern;

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
    private static int counter = 0; 
    private final CuratorFramework client;
    private final int instance;
    
    // Schema!
    // https://curator.apache.org/curator-framework/schema.html
    
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
        zkCreate(client, ZkConst.pRoot);
        zkCreate(client, ZkConst.pLogs);
        zkCreate(client, ZkConst.pStoreLock);
//        zkEnsure(client, ZkConst.pRoot);
//        zkEnsure(client, ZkConst.pStoreLock);
    }
    
    private void init() throws Exception {
        List<DataSourceDescription> x = listDataSources();
//        if ( x.isEmpty() )
//            LOGZK.info("<No logs>");
//        else
//            x.forEach(dsd->FmtLog.info(LOGZK, "Log: %s", dsd));
        lastSeen = new HashSet<>(x);
        watchForLogs();
    }

    // ---- Watching for logs
    // Watch for log changes.
    private Object watcherLock = new Object();
    private volatile Set<DataSourceDescription> lastSeen = Collections.emptySet(); 
    private Watcher patchLogWatcher = (event)->{
        // Even may be null if called from this process. 
        synchronized(watcherLock) {
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
            Set<DataSourceDescription> newLogs = scanForLogChanges();
            newLogs.forEach(dsd->
            // if does not exist.
                createPatchLog(dsd)
                );
        }
    }
    
    private Set<DataSourceDescription> scanForLogChanges() {
        Set<DataSourceDescription> nowSeen = new HashSet<>(listDataSources());
        // Access once.
        Set<DataSourceDescription> lastSeen$ = lastSeen;
        // Reload ASAP.
//        FmtLog.info(LOGZK, "** [%d] Watcher ** last: %s", instance, lastSeen);
//        FmtLog.info(LOGZK, "** [%d] Watcher ** now:  %s", instance, nowSeen);
        
        Set<DataSourceDescription> newLogs = SetUtils.difference(nowSeen, lastSeen$);
        Set<DataSourceDescription> deletedLogs = SetUtils.difference(lastSeen$, nowSeen);
        if ( ! newLogs.isEmpty() ) {
            FmtLog.info(LOGZK, "[%d] New: %s", instance, newLogs);
            newLogs.forEach(dsd->createPatchLog(dsd));
        }

        if ( ! deletedLogs.isEmpty() ) {
            FmtLog.info(LOGZK, "[%d] Deleted: ", instance, deletedLogs);
            deletedLogs.forEach(dsd->releasePatchLog(dsd.getId()));
        }
        return nowSeen;
    }
    // ---- Watching for logs
    
    @Override
    protected  List<DataSourceDescription> initialize(LocalServerConfig config) {
        if ( client == null )
            return Collections.emptyList();
        
        boolean isEmpty = zkCalc(()->client.checkExists().forPath(ZkConst.pRoot)==null);
        try {
            if ( isEmpty ) {
                LOGZK.info("No PatchStore - format new one"); 
                formatPatchStore(client);
            } else
                init();
            watchForLogs();
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
        List<DataSourceDescription> descriptions = new ArrayList<DataSourceDescription>();
        List<String> logNames = Zk.zkSubNodes(client, ZkConst.pLogs);
        logNames.forEach(name->{
            FmtLog.info(LOGZK, "[%d] listDataSources: %s", instance, name);
            String logPath = ZKPaths.makePath(ZkConst.pLogs, name);
            String logDsd = ZKPaths.makePath(ZkConst.pLogs, name, ZkConst.nDsd);
            JsonObject obj = zkFetchJson(client, logDsd);
            if ( obj == null )
                Zk.listNodes(client);
            
            DataSourceDescription dsd = DataSourceDescription.fromJson(obj);
            descriptions.add(dsd);
        });
        return descriptions;
    }
    
    @Override
    protected PatchLog create(DataSourceDescription dsd) {
        FmtLog.info(LOGZK,  "[%d] create patch log '%s'", instance, dsd.getName());
        
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
            zkLock(client, ZkConst.pStoreLock, ()->{
                formatLog(dsd, logPath);
            });
        }
        PatchLog patchLog = new PatchLogZk(dsd, instance, logPath, client, this);
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
}
