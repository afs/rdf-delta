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

import static org.seaborne.delta.DPConst.F_DATASOURCE;
import static org.seaborne.delta.DPConst.F_VERSION;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.*;
import org.seaborne.delta.lib.JSONX;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Track the state of one client connected to one {@code DataSource} */

public class DataState {
    // XXX Can this be shared between client and server?
    // DataSourceDescription
    
    static Logger LOG = LoggerFactory.getLogger(DataState.class);
    /*package*/ static String STATE_FILE = DPConst.STATE_CLIENT;
    
    private final Zone zone;
    private RefString state;

    // Persistent state that varies.
    private final AtomicInteger version = new AtomicInteger(-999);
    // Data source id for the state
    private Id datasource;
    
    // State is:
    //   version - at least an int
    //   dsRef - id
    //   (lastpatchId).
    // Stored as JSON:
    // {
    //   version:    int
    //   datasource: string/uuid
    //   patch:      string/uuid
    // }
    
//    public static DataState attach(Location filearea) {
//        // if creating ...
//        if ( ! filearea.isMem() ) {
//            if ( ! filearea.exists() )
//                FmtLog.warn(LOG, "Not such directory: %s", filearea);
//        }
//        Path path = Paths.get(filearea);
//        if ( ! Files.exists(path) ) {
//            LOG.error("No such directory: "+filearea);
//            throw new DeltaException("No such directory: "+filearea);
//        }
//        if ( ! Files.isDirectory(path) ) {
//            LOG.error("Not a directory: "+filearea);
//            throw new DeltaException("Not a directory: "+filearea);
//        }            
//
//        Path statePath = path.resolve(versionFile);
//        RefLong version = new PersistentState(statePath);
//
//        Path dataPath = path.resolve(versionFile);
//        if ( Files.exists(dataPath) ) {}
//
//         return new DataState 
//}

    // XXX Needs a lot of clean-up

    // XXX Zone object.
    // Global problems - per zone?
    
//    public static DataState attach(Id dsRef) {
//        DataState ds = Zone.states.get(dsRef);
//        if ( ds == null ) {
//            LOG.error("No state for "+dsRef);
//            throw new DeltaConfigException("No state for "+dsRef);
//        }
//        return ds;
//    }
//    
//    public static DataState attach(Location workspace, Id dsRef) {
////        if ( workspace.isMem() ) {
////            FmtLog.error(LOG, "Requires a disk-backed Location: %s", workspace);
////            throw new DeltaException("Requires a disk-back Location: "+workspace);
////        }
//        if ( ! workspace.exists() )
//            FmtLog.warn(LOG, "Not such directory: %s", workspace);
//        
//        return new DataState(dsRef, workspace);
//    }
//    
//    private DataState(Id dsRef, Location workspace) {
//        // Memory and disk versions.
//        this.workspace = workspace;
//        loadState(workspace, dsRef);
//        FmtLog.info(LOG, "%s : version = %s", dsRef, version);
//    }
//    
    
    /** Create from existing state */ 
    /*package*/ DataState(Zone zone, PersistentState state) {
        this.zone = zone;
        this.state = state;
        setStateFromString(this, state.getString());
        FmtLog.info(LOG, "%s : version = %s", datasource, version);
    }
    
    /** Create new, initialize state. */ 
    /*package*/ DataState(Zone zone, Path state, Id dsRef, int version) {
        this.zone = zone;
        this.state = new PersistentState(state);
        this.datasource = dsRef;
        this.version.set(version);
        writeState(this);
    }

    public int version() {
        return version.get();
    }

    public Zone zone() {
        return zone;
    }

    @Override
    public String toString() {
        return String.format("data state: %s version=%d", datasource, version());
    }
    
    // XXX Sort out concurrency!
    // XXX concurrency : Coordinate win DeltaConnection. 
    public synchronized void updateVersion(int version2) {
        // Update the shadow data first. Replaying patches is safe. 
        // Update on disk.
        writeState(this.state, this.datasource, version2);
        // Update local
        version.set(version2);
        // Failure: disk is ahead of memory: shadow data must already be updated.
        // Concurrency: app thinks at earlier version.
    }
    
    public Id getDataSourceId() {
        return datasource;
    }

    private static Map<Id, DataState> dataState = new ConcurrentHashMap<>();
    
    private void initData() {
//        sync();
//    }
//
//    private void sync() {
//        int ver = dConn.getRemoteVersionLatest();
//        
//        dConn.sync();
    }

//    private void loadState(Location workspace, Id dsRef) {
//        if ( workspace.isMem() ) {
//            loadStateEphemeral(workspace, dsRef);
//            return ;
//        }
//        loadStatePersistent(workspace);
//        return ;
//        
//    }
//    
//    private void loadStateEphemeral(Location workspace, Id dsRef) {
//        datasource = dsRef;
//        version.set(0);
//        String str = stateToString(datasource, version());
//        state = new RefStringMem(str);
//    }
//
//    private void loadStatePersistent(Location workspace) {
//        Path p = IOX.asPath(workspace);
//        if ( ! Files.exists(p) )
//            throw new DeltaConfigException("No directory: "+p);
//        if ( ! Files.isDirectory(p) )
//            throw new DeltaConfigException("Not a directory: "+p);
//        Path versionFile = p.resolve(STATE_FILE);
//        if ( ! Files.exists(versionFile) )
//            throw new DeltaConfigException("No state file: "+versionFile);
//        
//        state = new PersistentState(versionFile);
//        if ( state.getString().isEmpty() )
//            throw new DeltaConfigException("Error reading state: version file exist but is empty");  
//        setStateFromString(this, state.getString());
//    }

    private void readState(RefString state) {
        setStateFromString(this, state.getString());
    }

    
    private void writeState(DataState dataState) {
        writeState(this.state, dataState.datasource, dataState.version());
    }
    
    /** Allow a different version so we can write the state ahead of changing in-memory */  
    private static void writeState(RefString state, Id datasource, int version) {
        String x = stateToString(datasource, version);
        if ( ! x.endsWith("\n") )
            x = x+"\n";
        state.setString(x);
    }
    
    private static String stateToString(Id datasource, int version) {
        JsonObject json = stateToJson(datasource, version);
        return JSON.toString(json);
    }
    
    private static JsonObject stateToJson(Id datasource, int version) {
        return
            JSONX.buildObject(builder->{
                // --> pair.
                builder
                    .key(F_VERSION).value(version)
                    .key(F_DATASOURCE).value(datasource.asPlainString());
            });
    }
    
    private static void setStateFromString(DataState state, String string) {
        JsonObject obj = JSON.parse(string);
        setFromJsonObject(state, obj);
    }

    /** JsonObject -> DataState */
    private static void setFromJsonObject(DataState dataState, JsonObject sourceObj) {
        int version = JSONX.getInt(sourceObj, F_VERSION, -99);
        if ( version == -99 ) {
            LOG.warn("No version: "+JSON.toStringFlat(sourceObj));
        }
        dataState.version.set(version);

        String dsStr = JSONX.getStrOrNull(sourceObj, F_DATASOURCE);
        if ( dsStr != null )
            dataState.datasource = Id.fromString(dsStr);
        else {
            LOG.error("No datasource: "+JSON.toStringFlat(sourceObj));
            throw new DeltaException("No datasource: "+JSON.toStringFlat(sourceObj));
        }
    }
}
