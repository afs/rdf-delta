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

import static org.seaborne.delta.zk.Zk.zkPath;

import java.util.*;
import java.util.function.Supplier;

import org.apache.curator.framework.CuratorFramework;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.lib.ListUtils;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.zookeeper.Watcher;
import org.seaborne.delta.*;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.seaborne.delta.zk.Zk;
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
    private final String lockPath;
    private final String versionsPath;
    private final String headersPath;
    
    /* Keep head info - the (version, id, prev) is saved in /headers/<id> when a patch is stored in addition to the
     * /versions/NNNN which as just id.
     * This isn't necessary for operation. 
     * It can be used to check the patch store.
     */
    private final boolean keepHeaderInfo = true;
    
    /*
     * Verification can only happen if basic header information is stored.
     * Requires header information to have been kept.
     */
    // Could write a verifier that went to the PatchStorage to get header info.   
    private final boolean startupVerification = false && keepHeaderInfo ;
    
    private Version earliestVersion = Version.UNSET;
    private Id earliestId = null;
    
    // Set by newState
    private volatile long version = Version.UNSET.value();
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
        this.lockPath       = zkPath(logPath, ZkConst.nLock);
        this.versionsPath   = zkPath(logPath, ZkConst.nVersions);
        this.headersPath   = zkPath(logPath, ZkConst.nHeaders);

        // Find earliest.
        List<String> x = Zk.zkSubNodes(client, versionsPath);
        //Guess: 1
        if ( x.isEmpty() )
            earliestVersion = Version.INIT;
        else if ( x.contains("00000001") )
            // Fast-track the "obvious" answer
            earliestVersion = Version.create(1);
        else {
            try {
                long ver = x.stream().map(this::versionFromName).filter(v->(v>0)).min(Long::compare).get();
                earliestVersion = Version.create(ver);
            } catch (NoSuchElementException ex) {  }
        }
        earliestId = versionToId(earliestVersion);
        // Initialize, start watching
        stateOrInit();
        
        if ( startupVerification ) {
            if ( ! keepHeaderInfo )
                FmtLog.warn(LOG, "Not keeping header information. Verification may fail if any previous runs also did not keep header information."); 
            verify(client, x);
        }
    }

    private void verify(CuratorFramework client, List<String> versions) {
        List<String> headers = Zk.zkSubNodes(client, headersPath);
        
        if ( versions.isEmpty() ) {
            if ( ! headers.isEmpty() ) { /*msg*/ }
            if ( version != Version.INIT.value() ) { /*msg*/ }
            if ( current != null ) { /*msg*/ }
            if ( previous != null ) { /*msg*/ }
            return ;
        }
        // Create the threaded patches
        Map<Id, Id> mapIdToPrev = new HashMap<>();
        Map<Id, Id> mapPrevToId = new HashMap<>();
        Map<Long, Id> mapLongToId = new HashMap<>();
        
        headers.forEach(idStr->{
            Id id = Id.fromString(idStr);
            String path = zkPath(headersPath, idStr);
            JsonObject obj = Zk.zkFetchJson(client, path);
            Id patchId = getIdOrNull(obj, fId);
            if ( ! Objects.equals(id, patchId) ) { /*msg*/ }
            
            Id prevId = getIdOrNull(obj, fPrevious);
            long ver = JSONX.getLong(obj, fVersion, -99);
            if ( patchId == null ) {
                FmtLog.error(LOG, "Null patch id (%s, %s)", patchId, prevId);
                return;
            }
            if ( mapIdToPrev.containsKey(patchId) )
                FmtLog.error(LOG, "Duplicate for %s: was %s : now %s", patchId, mapIdToPrev.get(patchId), prevId);
            
            mapIdToPrev.put(patchId, prevId);
            mapPrevToId.put(prevId, patchId);
            mapLongToId.put(ver, patchId);
        });
        
        // Find all the ids with no prev. 
        List<Id> firsts = ListUtils.toList(mapIdToPrev.entrySet().stream().filter(e->e.getValue()==null).map(e->e.getKey()));
        if ( firsts.isEmpty() ) {
            FmtLog.error(LOG, "No initial patch found");
            return;
        }
        if ( firsts.size() > 2 ) {
            FmtLog.error(LOG, "Multiple patchs with no prev: %s", firsts);
            return;
        }
        // Off by one!
        List<Id> versionsCalc = new ArrayList<>(); 
        Id initialId = firsts.get(0);
        Id id = initialId;
        long count = 0;
        
        for(;;) {
            versionsCalc.add(id);
            Id next = mapPrevToId.get(id);
            if ( next == null )
                break;
            id = next;
        }
        
        if ( versionsCalc.size() != versions.size() ) { /*msg*/ }
        if ( versionsCalc.size()+1 != version ) { /*msg*/ }
        
        Id mostRecent = versionsCalc.get(versionsCalc.size()-1);
        if ( ! mostRecent.equals(current) ) { /*msg*/ }
        
        if ( previous != null ) {
            if ( versionsCalc.size() == 1 ) {}
            Id mostRecentPrev = versionsCalc.get(versionsCalc.size()-2);
            if ( mostRecentPrev.equals(previous) ) { /*msg*/ }
        } else {
            if ( versionsCalc.size() != 1 ) { /*msg*/ }
        }
    }

    

    // ---- Zookeeper index state watching.
    
    @Override
    public void refresh() {
        //loadState(false);
    }
    
    // ---- Zookeeper index state watching.
    
    @Override
    public void delete() {
        // Don't actually delete the state.
    }

    @Override
    public void release() {
        // Release local resources.
    }

    @Override
    public boolean isEmpty() {
        //return version == DeltaConst.VERSION_UNSET || DeltaConst.VERSION_INIT;
        return current == null;
    }

    @Override
    public Version nextVersion() {
        Version ver = Version.create(version+1);
        FmtLog.debug(LOG, "Alloc version %d", ver);
        return ver;
    }

    // ---- Zookeeper index state watching.
    
    private void newState(long newVersion, Id patch, Id prev) {
        synchronized(lock) {
            FmtLog.debug(LOG, "newVersion %d->%d", version, newVersion);
            if ( newVersion <= version ) {
                if ( newVersion == version ) {
                    if ( ! Objects.equals(patch, current) || ! Objects.equals(prev,previous) )
                        FmtLog.error(LOG, "Same version but different ids: current=(%s,%s), proposed=(%s,%s)", current, previous, patch, prev);
                }
                return ;
            }
            
            //FmtLog.debug(LOG, "-- [%d] State: [%s] (%s, %s, %s)", instance, dsd.getName(), version, current, previous);
            if ( newVersion == DeltaConst.VERSION_FIRST && ! earliestVersion.isValid() ) {
                // If going no patch -> patch, set the start.
                earliestVersion = Version.FIRST;
                earliestId = patch;
            }
            this.version = newVersion;
            this.current = patch;
            this.previous = prev;
        }
    }

    @Override
    public void save(Version version, Id patch, Id prev) {
        // Should always be called inside the patch lock.
        save(version.value(), patch, prev);
    }

    private void save(long version, Id patch, Id prev) {
        newState(version, patch, prev);
        JsonObject x = stateToJson(version, patch, prev);
        byte[] bytes = JSONX.asBytes(x);
        if ( patch != null ) {
            // [META]
            // Record the basic header - (version, id, prev) - for validation.   
            if ( keepHeaderInfo )
                Zk.zkCreateSet(client, headerPath(patch), bytes);
            // Write version->id mapping.
            Zk.zkCreateSet(client, versionPath(version), patch.asBytes());
        }
        Zk.zkSet(client, statePath, bytes);
    }

    private Watcher logStateWatcher = (event)->{
        synchronized(lock) {
            FmtLog.debug(LOG, "++ [%d:%s] Log watcher", instance, logName);
            syncState();
        }
    };

    private void syncState() {
        JsonObject obj = getWatchedState();
        if ( obj != null )
            jsonSetState(obj);
    }

    private JsonObject getWatchedState() {
        return Zk.zkFetchJson(client, logStateWatcher, statePath);
    }

    // ---- Zookeeper index state watching.

    private void stateOrInit() {
        synchronized(lock) {
            JsonObject obj = getWatchedState();
            if ( obj != null )
                jsonSetState(obj);
            else
                initState();
            if ( version == DeltaConst.VERSION_UNSET )
                save(DeltaConst.VERSION_INIT, current, previous);
        }
    }

    private void initState() {
        runWithLock(()->{
            FmtLog.debug(LOG, "initState %s", logName);
            if ( current == null )
                earliestVersion = Version.INIT;
            version = DeltaConst.VERSION_INIT;
            save(version, current, previous);
        });
    }

    /** The initial state object of a patch log */ 
    /*package*/ static JsonObject initialStateJson() {
        FmtLog.debug(LOG, "initialStateJson");
        return JSONX.buildObject(b->{
            b.pair(fVersion, DeltaConst.VERSION_INIT);
        }); 
    }
    
    private static JsonObject stateToJson(long version, Id patch, Id prev) {
        FmtLog.debug(LOG, "stateToJson ver=%d", version);
        return JSONX.buildObject(b->{
            b.pair(fVersion, version);
            if ( patch != null )
                b.pair(fId, patch.asPlainString());
            if ( prev != null )
                b.pair(fPrevious, patch.asPlainString());
        }); 
    }
    
    private void jsonSetState(JsonObject obj) {
        try {
            FmtLog.debug(LOG, "jsonToState %s",JSON.toStringFlat(obj));
            long ver = obj.get(fVersion).getAsNumber().value().longValue();
            if ( ver == version )
                return ;
            Id newCurrent = getIdOrNull(obj, fId);
            Id newPrevious = getIdOrNull(obj, fPrevious);
            newState(ver, newCurrent, newPrevious);
        } catch (RuntimeException ex) {
            FmtLog.error(this.getClass(), "Failed to load the patch log index state", ex);
        }
    }

    @Override
    public Version getCurrentVersion() {
        return Version.create(version);
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
    public Version getEarliestVersion() {
        return earliestVersion;
    }

    @Override
    public Id getEarliestId() {
        return earliestId;
    }

    @Override
    public Id versionToId(Version ver) {
        // Cache?
        if ( ! Version.isValid(ver) )
            return null;
        String p = versionPath(ver);
        byte[] b = Zk.zkFetch(client, versionPath(ver));
        if ( b == null )
            return null;
        Id id = Id.fromBytes(b);
        return id;
    }

    @Override
    public PatchInfo getPatchInfo(Id id) {
        String p = headerPath(id);
        JsonObject obj = Zk.zkFetchJson(client, p);
        Id patchId = getIdOrNull(obj, fId);
        if ( ! Objects.equals(id, patchId) ) { /*msg*/ }
        Id prevId = getIdOrNull(obj, fPrevious);
        long ver = JSONX.getLong(obj, fVersion, -99);
        Version version = ver < 0 ? Version.UNSET : Version.create(ver);
        return new PatchInfo(patchId, Version.UNSET, prevId);
    }

    private String versionPath(Version ver) { return versionPath(ver.value()) ; }
    private String versionPath(long ver) { return Zk.zkPath(versionsPath, String.format("%08d", ver)); }
    private String headerPath(Id id) { return Zk.zkPath(headersPath, id.asPlainString()); }
    
    private long versionFromName(String name) {
        try {
            return Long.parseLong(name);
        } catch (NumberFormatException ex) {
            Log.warn(this, "Attempt to extract the version from '"+name+"'");
            return -1;
        }
    }

    private Id getIdOrNull(JsonObject obj, String field) {
        String s = JSONX.getStrOrNull(obj, field);
        if ( s == null )
            return null;
        return Id.fromString(s);
    }

    @Override
    public void runWithLock(Runnable action) {
        synchronized(lock) {
            Zk.zkLock(client, lockPath, ()->{
                try {
                    action.run();
                } 
                catch(DeltaException ex) { throw ex; }
                catch(RuntimeException ex) {
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
                } 
                catch(DeltaException ex) { throw ex; }
                catch(RuntimeException ex) {
                    FmtLog.warn(LOG, "RuntimeException in runWithLock");
                    ex.printStackTrace();
                    throw ex;
                }
            });
        }
    }
}
