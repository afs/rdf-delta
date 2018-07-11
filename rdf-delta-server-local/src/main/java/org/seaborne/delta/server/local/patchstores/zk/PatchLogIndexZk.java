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

import java.util.List;
import java.util.NoSuchElementException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonValue;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.zookeeper.Watcher;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.Id;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** State control for a {@link PatchStore} */
public class PatchLogIndexZk implements PatchLogIndex {
    private static Logger LOG = LoggerFactory.getLogger(PatchLogIndexZk.class);
    
    private final Object lock = new Object();
    private final CuratorFramework client;

    private int instance;

    private final DataSourceDescription dsd;
    private String logName;  // Java-ism - can't use the DSD final in Watcher lambdas 
    private final String statePath;
    private final String versionsPath;
    
    private long earliestVersion = DeltaConst.VERSION_UNSET;
    private Id earliestId = null;
    
    private long version = DeltaConst.VERSION_UNSET;
    private Id current = null;
    private Id previous = null;
    
    private final String fVersion = "version";
    private final String fId = "id";
    private final String fPrevious = "previous";

    /** {@code PatchLogIndexZk}
     * <ul>
     * <li>{@code statePath} - where the patches are stored.
     * <li>{@code versionsPath} - directory where the meta data JSON object is stored as name "%08d"
     * </ul> 
     */
    
    public PatchLogIndexZk(CuratorFramework client, int instance, DataSourceDescription dsd, String statePath, String versionsPath) {
        this.client = client ;
        this.instance = instance;
        this.dsd = dsd;
        this.logName = dsd.getName();
        Zk.zkEnsure(client, statePath);
        Zk.zkEnsure(client, versionsPath);
        this.statePath = statePath;
        this.versionsPath = versionsPath;

        // Find earliest.
        List<String> x = Zk.zkSubNodes(client, versionsPath);
        //Guess: 1
        if ( x.isEmpty() )
            earliestVersion = DeltaConst.VERSION_INIT;
        else if ( x.contains("00000001") )
            // Fast-track the "obvious" answer
            earliestVersion = 1;
        else {
            try {
                earliestVersion = x.stream().map(this::versionFromName).filter(v->(v>0)).min(Long::compare).get();
            } catch (NoSuchElementException ex) {  }
        }
        earliestId = mapVersionToId(earliestVersion);
        // Initialize, start watching
        stateOrInit();
    }

    private Id getIdOrNull(JsonObject obj, String field) {
        JsonValue jv = obj.get(field);
        if ( jv == null )
            return null;
        String s = jv.getAsString().value();
        return Id.fromString(s);
    }

    @Override
    public boolean isEmpty() {
        //return version == DeltaConst.VERSION_UNSET || DeltaConst.VERSION_INIT;
        return current == null;
    }

    @Override
    public long nextVersion() {
        return version+1;
    }

    // ---- Zookeeper index state watching.
    
    // XXX Migrate the test
    private void newVersion(long newVersion) {
        version = newVersion;
        if ( newVersion >= DeltaConst.VERSION_FIRST && DeltaConst.versionNoPatches(earliestVersion) )
            // If going no patch -> patch, set the start.
            earliestVersion = DeltaConst.VERSION_FIRST;
    }

    @Override
    public void save(long version, Id patch, Id prev) {
        newVersion(version);
        this.current = patch;
        this.previous = prev;
        JsonObject x = stateToJson(version, patch, prev);
        if ( patch != null )
            Zk.zkCreateSet(client, versionPath(version), patch.asBytes());
        Zk.zkSetJson(client, statePath, x);
    }

    @Override
    public void refresh() {
        //loadState(false);
    }
    
    // ---- Zookeeper index state watching.

    private Watcher logStateWatcher = (event)->{
        synchronized(lock) {
            //FmtLog.info(LOG, "++ [%d:%s] Log watcher", instance, logName);
            syncState();
        }
    };

    private void syncState() {
        JsonObject obj = getWatchState();
        if ( obj != null )
            jsonToState(obj);
    }

    private void stateOrInit() {
        JsonObject obj = getWatchState();
        if ( obj != null )
            jsonToState(obj);
        else
            initState();
    }

    private void initState() {
        if ( current == null ) {
                save(DeltaConst.VERSION_INIT, null, null);
            earliestVersion = DeltaConst.VERSION_INIT;
        } else {
            save(version, current, previous); 
        }
    }
    
    private void loadState(boolean init) {
        JsonObject obj = Zk.zkFetchJson(client, statePath);
        if ( obj == null )
            return;
        jsonToState(obj);
    }
    
    private JsonObject getWatchState() {
        return Zk.zkFetchJson(client, logStateWatcher, statePath);
    }

    private JsonObject stateToJson(long version, Id patch, Id prev) {
        return JSONX.buildObject(b->{
            b.pair(fVersion, version);
            if ( patch != null )
                b.pair(fId, patch.asPlainString());
            if ( prev != null )
                b.pair(fPrevious, patch.asPlainString());
        }); 
    }
    
    private void jsonToState(JsonObject obj) {
        try {
            long ver = obj.get(fVersion).getAsNumber().value().longValue();
            if ( ver == version )
                return ;
            newVersion(ver);
            if ( version >= DeltaConst.VERSION_FIRST && DeltaConst.versionNoPatches(earliestVersion) ) { 
                 // If going no patch -> patch, set the start.
                earliestVersion = DeltaConst.VERSION_FIRST;
            }
            current = getIdOrNull(obj, fId);
            previous = getIdOrNull(obj, fPrevious);
            FmtLog.info(LOG, "-- [%d] State: [%s] (%s, %s, %s)", instance, dsd.getName(), version, current, previous);
        } catch (RuntimeException ex) {
            FmtLog.info(this.getClass(), "Failed to load the patch log index state", ex);
        }
    }

    @Override
    public long getCurrentVersion() {
        return version;
    }

    @Override
    public Id getCurrentId() {
        return current;
    }

    @Override
    public Id getPreviousId() {
        return previous;
    }

    @Override
    public long getEarliestVersion() {
        return earliestVersion;
    }

    @Override
    public Id getEarliestId() {
        return earliestId;
    }

    @Override
    public Id mapVersionToId(long ver) {
        if ( ver == DeltaConst.VERSION_INIT || ver == DeltaConst.VERSION_UNSET )
            return null;
        String p = versionPath(ver);
        byte[] b = Zk.zkFetch(client, versionPath(ver));
        if ( b == null )
            return null;
        Id id = Id.fromBytes(b);
        return id;
    }

//    @Override
//    public long mapIdToVersion(Id id) {
//        throw new UnsupportedOperationException();
//    }

    private String versionPath(long ver) { return Zk.zkPath(versionsPath, String.format("%08d", ver)); }

    private long versionFromName(String name) {
        try {
            return Long.parseLong(name);
        } catch (NumberFormatException ex) {
            Log.warn(this, "Attempt to extract the version from '"+name+"'");
            return -1;
        }
    }

    @Override
    public void release() {
        // XXX Zk lock needed.
        Zk.zkDelete(client, statePath);
        Zk.zkDelete(client, versionsPath);
    }
}
