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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.lib.NotImplemented;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.*;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.lib.JSONX;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Track the state of one client connected to one {@code DataSource} */

public class DataState {
    // XXX Can this be shared between client and server?
    // DataSourceDescription
    
    private static Logger LOG = LoggerFactory.getLogger(DataState.class);
    private static String VERSION_FILE = "version";
    
    private final DeltaConnection dConn;
    private final Id dsRef;
    private final Location workspace;
    private RefString state;

    // Persistent state that varies.
    private final AtomicInteger version = new AtomicInteger(-999);
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

    /** Intialize */ 
    public static void format(Location workspace) {
        if ( workspace.isMem() )
            return;
        if ( ! workspace.exists() )
            FmtLog.warn(LOG, "Not such directory: %s", workspace);

        //FileOps.ensureDir(workspace.getDirectoryPath());
        // LOC/data/ then data.ttl or TDB database.
        // LOC/state
    }
    
    public /*for now*/ DataState(DeltaConnection dConn, Id dsRef, Location workspace) {
        this.dConn = dConn;
        this.dsRef = dsRef;
        // Memory and disk versions.
        this.workspace = workspace;
        // Load version
        loadState(workspace);
        FmtLog.info(LOG, "%s : version = %s", dsRef, version);
    }
    
    public int version() {
        return version.get();
    }

    // XXX Sort out concurrency!
    // XXX concurrency : Coordinate win DeltaConnection. 
    public synchronized void updateVersion(int version2) {
        // Update in deltya connection first so sync() does not attempt to update the shadow.  
        //dConn.setLocalVersionNumber(version2);
        // Update on disk.
        writeState(this, version2);
        // and then our local version.
        version.set(version2);
    }
    
    private void initData() {
//        sync();
//    }
//
//    private void sync() {
//        int ver = dConn.getRemoteVersionLatest();
//        
//        dConn.sync();
    }

    private void loadState(Location workspace) {
        if ( workspace.isMem() ) {
            loadStateEphemeral(workspace);
            return ;
        }
        // Persistent.
        // data?
        loadStatePersistent(workspace);
        return ;
        
    }
    
    private void loadStateEphemeral(Location workspace) {
        version.set(0);
        datasource = dsRef;
        JsonObject json = stateToJson(this, version());
        String str = JSON.toString(json);
        state = new RefStringMem(str);
    }

    private void loadStatePersistent(Location workspace) {
        Path p = IOX.asPath(workspace);
        Path versionFile = p.resolve(VERSION_FILE);
        state = new PersistentState(versionFile);
        if ( state.getString().isEmpty() ) {
            version.set(0);
            datasource = dsRef;
            writeState(this);
        } else
            setStateFromString(this, state.getString());
    }

    private void writeState(DataState dataState) {
        writeState(dataState, dataState.version());
    }
    
    /** Allow a different version so we can wrietteh state ahead of changing in-memory */  
    private void writeState(DataState dataState, int version) {
        String x = stateToString(dataState, version);
        if ( ! x.endsWith("\n") )
            x = x+"\n";
        state.setString(x);
    }
    
    private static String stateToString(DataState state) {
        JsonObject json = stateToJson(state, state.version());
        return JSON.toString(json);
    }
    
    private static String stateToString(DataState state, int version) {
        JsonObject json = stateToJson(state, version);
        return JSON.toString(json);
    }
    
    private static DataState setStateFromString(DataState state, String string) {
        JsonObject obj = JSON.parse(string);
        return setFromJsonObject(state, obj);
    }

    private static JsonObject stateToJson(DataState state, int version) {
        return
            JSONX.buildObject(builder->{
                // --> pair.
                builder
                    .key(F_VERSION).value(version)
                    .key(F_DATASOURCE).value(state.datasource.asPlainString());
            });
    }
    
    /** JsonObject -> SourceDescriptor */
    private static DataState setFromJsonObject(DataState dataState, JsonObject sourceObj) {
        int version = JSONX.getInt(sourceObj, F_VERSION, -99);
        if ( version == -99 ) {
            LOG.warn("No version: "+JSON.toStringFlat(sourceObj));
        }
        dataState.version.set(version);

        String dsStr = JSONX.getStrOrNull(sourceObj, F_DATASOURCE);
        if ( dsStr != null )
            dataState.datasource = Id.fromString(dsStr); 
        
        return dataState;
    }
}
