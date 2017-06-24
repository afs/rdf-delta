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

import static org.seaborne.delta.DeltaConst.* ;
import static org.seaborne.delta.DeltaConst.F_ID ;
import static org.seaborne.delta.DeltaConst.F_VERSION;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    /*package*/ static String STATE_FILE = DeltaConst.STATE_CLIENT;
    
    private final Zone zone;
    private String name;
    private String uri;
    private RefString stateStr;
    private PersistentState state;

    // Data source id for the state
    private Id datasource;
    
    // Persistent state that varies.
    private long version = -999;
    private Id patchId = null;
    
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
    
    /** Create from existing state */ 
    /*package*/ DataState(Zone zone, PersistentState state) {
        this.zone = zone;
        this.state = state;
        this.stateStr = state;
        setStateFromString(this, state.getString());
        FmtLog.info(LOG, "%s : version = %s", datasource, version);
    }
    
    /** Create new, initialize state. */ 
    /*package*/ DataState(Zone zone, Path stateFile, Id dsRef, String name, String uri, int version, Id patchId) {
        this.zone = zone;
        this.datasource = dsRef;
        this.state = null;
        if ( stateFile != null ) 
            this.state = new PersistentState(stateFile);
        this.stateStr = state;
        this.version = version;
        this.patchId = patchId;
        this.name = name;
        this.uri = uri;
        writeState(this);
    }

    public void refresh() {
        if ( state != null )
            load(getStatePath());
    }
    
    private void load(Path stateFile) {
        state = new PersistentState(stateFile);
        stateStr = state;
        readState(stateStr);
    }

    public long version() {
        return version;
    }

    public Id latestPatchId() {
        return patchId;
    }

    public Zone zone() {
        return zone;
    }

    @Override
    public String toString() {
        return String.format("data state: %s version=%d id=%s", datasource, version(), latestPatchId());
    }
    
    // XXX Sort out concurrency!
    // XXX concurrency : Coordinate win DeltaConnection. 
    public synchronized void updateState(long newVersion, Id patchId) {
        // Update the shadow data first. Replaying patches is safe. 
        // Update on disk.
        writeState(this.stateStr, this.datasource, this.name, this.uri, newVersion, patchId);
        // Update local
        this.version = newVersion;
        this.patchId = patchId;
    }
    
    public Id getDataSourceId() {
        return datasource;
    }

    /** Place on-disk where the state is stored. Use with care. */ 
    public Path getStatePath() {
        return state.getPath();
    }

    public String getName() {
        return name ;
    }

    public void setName(String name) {
        this.name = name ;
    }

    public String getUri() {
        return uri ;
    }

    public void setUri(String uri) {
        this.uri = uri ;
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
        if ( dataState.state != null )
            writeState(this.stateStr, dataState.datasource, dataState.name, dataState.uri, dataState.version, dataState.patchId);
    }
    
    /** Allow a different version so we can write the state ahead of changing in-memory */  
    private static void writeState(RefString state, Id datasource, String name, String uri, long version, Id patchId) {
        String x = stateToString(datasource, name, uri, version, patchId);
        if ( ! x.endsWith("\n") )
            x = x+"\n";
        state.setString(x);
    }
    
    private static String stateToString(Id datasource, String name, String uri, long version, Id patchId) {
        JsonObject json = stateToJson(datasource, name, uri, version, patchId);
        return JSON.toString(json);
    }
    
    private static JsonObject stateToJson(Id datasource, String name, String uri, long version, Id patchId) {
        String x = "";
        if ( patchId != null )
            x = patchId.asString();
        String patchStr = x;
        return
            JSONX.buildObject(builder->{
                builder
                    .key(F_VERSION).value(version)
                    .key(F_ID).value(patchStr)
                    .key(F_NAME).value(name)
                    .key(F_DATASOURCE).value(datasource.asPlainString());
                if ( uri != null )
                    builder.key(F_URI).value(uri);
            });
    }
    
    /** Set version and datasource id from a string which is JOSN */
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
        dataState.version = version;
        
        String patchStr = JSONX.getStrOrNull(sourceObj, F_ID);
        if ( patchStr == null || patchStr.isEmpty() ) {
            dataState.patchId = null;
        } else {
            dataState.patchId = Id.fromString(patchStr);
        }
            
        String dsStr = JSONX.getStrOrNull(sourceObj, F_DATASOURCE);
        if ( dsStr != null )
            dataState.datasource = Id.fromString(dsStr);
        else {
            LOG.error("No datasource: "+JSON.toStringFlat(sourceObj));
            throw new DeltaException("No datasource: "+JSON.toStringFlat(sourceObj));
        }
        
        String name = JSONX.getStrOrNull(sourceObj, F_NAME);
        if ( name != null )
            dataState.name = name;
        else {
            LOG.error("No datasource name: "+JSON.toStringFlat(sourceObj));
            throw new DeltaException("No datasource name: "+JSON.toStringFlat(sourceObj));
        }
        
        String uri = JSONX.getStrOrNull(sourceObj, F_URI);
        if ( uri != null )
            dataState.uri = uri;
    }
}
