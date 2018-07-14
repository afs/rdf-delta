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

import static org.seaborne.delta.server.local.patchstores.zk.Zk.zkPath;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.curator.framework.CuratorFramework;
import org.apache.jena.atlas.json.JSON;
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

    private /*final*/ int instance;

    private final DataSourceDescription dsd;
    private String logName;  // Java-ism - can't use the DSD final in Watcher lambdas 
    private final String statePath;
    private final String versionsPath;
    private final String lockPath;
    
    
    private long earliestVersion = DeltaConst.VERSION_UNSET;
    private Id earliestId = null;
    
    // Set by jsonToState.
    // XXX Make sync.
    private volatile long version = DeltaConst.VERSION_UNSET;
    private volatile Id current = null;
    private volatile Id previous = null;

    private static final String fVersion = "version";
    private static final String fId = "id";
    private static final String fPrevious = "previous";

    /** {@code PatchLogIndexZk}
     * <ul>
     * <li>{@code statePath} - where the patches are stored.
     * <li>{@code versionsPath} - directory where the meta data JSON object is stored as name "%08d"
     * </ul> 
     */
    
    public PatchLogIndexZk(CuratorFramework client, int instance, DataSourceDescription dsd, String logPath) {
        this.client = client ;
        this.instance = instance;
        this.dsd = dsd;
        this.logName = dsd.getName();
        this.statePath      = zkPath(logPath, ZkConst.nState);
        this.versionsPath   = zkPath(logPath, ZkConst.nVersions);
        this.lockPath       = zkPath(logPath, ZkConst.nLock);
        Zk.zkEnsure(client, statePath);
        Zk.zkEnsure(client, versionsPath);
        Zk.zkEnsure(client, lockPath);

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

    @Override
    public boolean isEmpty() {
        //return version == DeltaConst.VERSION_UNSET || DeltaConst.VERSION_INIT;
        return current == null;
    }

    @Override
    public long nextVersion() {
        // XXX Inside the cluster lock.
        // Need parent lock cycle call down.
        FmtLog.info(LOG, "Alloc version %d", (version+1));
        return version+1;
    }

    // ---- Zookeeper index state watching.
    
    private void newState(long newVersion, Id patch, Id prev) {
        synchronized(lock) {
            FmtLog.info(LOG, "newVersion %d->%d", version, newVersion);
            if ( newVersion <= version ) {
                if ( newVersion == version ) {
                    if ( ! Objects.equals(patch, current) || ! Objects.equals(prev,previous) )
                        FmtLog.error(LOG, "Same version but different ids: current=(%s,%s), proposed=(%s,%s)", current, previous, patch, prev);
                } else 
                    FmtLog.info(LOG, "newVersion: no change", version, newVersion);
                return ;
            }
            
            //FmtLog.debug(LOG, "-- [%d] State: [%s] (%s, %s, %s)", instance, dsd.getName(), version, current, previous);
            // XXX iff newVersion > version. 
            version = newVersion;
            if ( newVersion >= DeltaConst.VERSION_FIRST && DeltaConst.versionNoPatches(earliestVersion) )
                // If going no patch -> patch, set the start.
                earliestVersion = DeltaConst.VERSION_FIRST;
            this.current = patch;
            this.previous = prev;
            if ( earliestId == null )
                earliestId = patch;
        }
    }

    @Override
    public void save(long version, Id patch, Id prev) {
        // Log wide lock?
        newState(version, patch, prev);
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
            FmtLog.info(LOG, "++ [%d:%s] Log watcher", instance, logName);
            syncState();
        }
    };

    private void syncState() {
        JsonObject obj = getWatchedState();
        if ( obj != null )
            jsonToState(obj);
    }

    private JsonObject getWatchedState() {
        return Zk.zkFetchJson(client, logStateWatcher, statePath);
    }

    // ---- Zookeeper index state watching.

    private void stateOrInit() {
        synchronized(lock) {
            JsonObject obj = getWatchedState();
            if ( obj != null )
                jsonToState(obj);
            else
                initState();
        }
    }

    private void initState() {
        FmtLog.info(LOG, "initState %s", logName);
        if ( current == null )
            earliestVersion = DeltaConst.VERSION_INIT;
        version = DeltaConst.VERSION_INIT;
        save(version, current, previous); 
    }
    
//    private void loadState(boolean init) {
//        JsonObject obj = Zk.zkFetchJson(client, statePath);
//        if ( obj == null )
//            return;
//        jsonToState(obj);
//    }
    
    private static JsonObject stateToJson(long version, Id patch, Id prev) {
        FmtLog.info(LOG, "stateToJson ver=%d", version);
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
            FmtLog.info(LOG, "jsonToState %s",JSON.toStringFlat(obj));
            long ver = obj.get(fVersion).getAsNumber().value().longValue();
            if ( ver == version )
                return ;
            Id newCurrent = getIdOrNull(obj, fId);
            Id newPrevious = getIdOrNull(obj, fPrevious);
            newState(ver, newCurrent, newPrevious);
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

    private Id getIdOrNull(JsonObject obj, String field) {
        JsonValue jv = obj.get(field);
        if ( jv == null )
            return null;
        String s = jv.getAsString().value();
        return Id.fromString(s);
    }

    @Override
    public void release() {
        // XXX Zk lock needed?
        Zk.zkDelete(client, statePath);
        Zk.zkDelete(client, versionsPath);
    }

    @Override
    public void runWithLock(Runnable action) {
        synchronized(lock) {
            Zk.zkLock(client, lockPath, ()->{
                try {
                    action.run();
                } catch(RuntimeException ex) {
                    FmtLog.warn(LOG, "RuntimeException in runWithLock");
                    ex.printStackTrace();
                    throw ex;
                }
            });
        }
    }
    
    @Override
    public <X> X runWithLockRtn(Supplier<X> action) {
        synchronized(lock) {
            return Zk.zkLockRtn(client, lockPath, ()->{
                try {
                    return action.get();
                } catch(RuntimeException ex) {
                    FmtLog.warn(LOG, "RuntimeException in runWithLock");
                    ex.printStackTrace();
                    throw ex;
                }
            });
        }
    }
}
