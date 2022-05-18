/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.server.local.patchstores.zk;

import static org.seaborne.delta.zk.Zk.zkPath;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.lib.DateTimeUtils;
import org.apache.jena.atlas.lib.ListUtils;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.zookeeper.Watcher;
import org.seaborne.delta.*;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.server.local.JsonLogEntry;
import org.seaborne.delta.server.local.LogEntry;
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

    private /*final*/ String instance;

    private final DataSourceDescription dsd;
    private final String logName;
    private final String statePath;
    private final String lockPath;
    private final String lockStatePath;
    private final String versionsPath;
    private final String headersPath;
    private final InterProcessLock zkLock;

    // null => no watching.
    //private Watcher logStateWatcher = null;
    private final Watcher logStateWatcher;

    /* Keep header info - the (version, id, prev) is saved in /headers/<id> when a patch is stored in addition to the
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

    /** {@code PatchLogIndexZk}
     * <ul>
     * <li>{@code statePath} - where the patches are stored.
     * <li>{@code versionsPath} - directory where the meta data JSON object is stored as name "%08d"
     * </ul>
     */

    private static AtomicInteger counter = new AtomicInteger(0);

    public PatchLogIndexZk(CuratorFramework client, String instance, DataSourceDescription dsd, String logPath) {
        // THis gets called twice in the creator - sees its own create via ZK watcher.
        this.client = client ;
        this.instance = instance;
        this.dsd = dsd;
        this.logName = dsd.getName();
        this.statePath      = zkPath(logPath, ZkConst.nState);
        this.lockPath       = zkPath(logPath, ZkConst.nLock);
        this.lockStatePath  = zkPath(logPath, ZkConst.nLockState);
        this.versionsPath   = zkPath(logPath, ZkConst.nVersions);
        this.headersPath    = zkPath(logPath, ZkConst.nHeaders);
        this.logStateWatcher = (event)->{
          synchronized(lock) {
              FmtLog.debug(LOG, "++ [%s:%s] Log watcher", instance, logName);
              syncState();
          }
        };
        this.zkLock = Zk.zkCreateLock(client, lockPath);

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
                long ver = x.stream().map(this::versionFromName).filter(v->(v>0)).min(Long::compare).orElseThrow();
                earliestVersion = Version.create(ver);
            } catch (NoSuchElementException ex) {
                FmtLog.warn(LOG, "Failed to find the earliest value when there is at least one version");
            }
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
        Map<Version, Id> mapVersionToId = new HashMap<>();

        headers.forEach(idStr->{
            Id id = Id.fromString(idStr);
            String path = zkPath(headersPath, idStr);
            JsonObject obj = Zk.zkFetchJson(client, path);
            LogEntry entry = JsonLogEntry.jsonToLogEntry(obj);
            Id patchId = entry.getPatchId();
            //if ( ! Objects.equals(id, patchId) ) { /*msg*/ }

            Id prevId = entry.getPrevious();
            Version ver = entry.getVersion();
            if ( patchId == null ) {
                FmtLog.error(LOG, "Null patch id (%s, %s)", patchId, prevId);
                return;
            }
            if ( mapIdToPrev.containsKey(patchId) )
                FmtLog.error(LOG, "Duplicate for %s: was %s : now %s", patchId, mapIdToPrev.get(patchId), prevId);

            mapIdToPrev.put(patchId, prevId);
            mapPrevToId.put(prevId, patchId);
            mapVersionToId.put(ver, patchId);
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

    @Override
    public boolean isEmpty() {
        //return version == Version.UNSET || version == Version.INIT;
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
            FmtLog.debug(LOG, "newVersion %d -> %d", version, newVersion);
            //FmtLog.info(LOG, "[%s] newState %s -> %s", instance,  Version.str(version), Version.str(newVersion));
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
            // newVersion > version
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

    private void syncState() {
        JsonObject obj = getWatchedState();
        if ( obj != null )
            jsonSetState(obj);
    }

    private JsonObject getWatchedState() {
        return Zk.zkFetchJson(client, logStateWatcher, statePath);
    }

    @Override
    public void syncVersionInfo() {
        syncState();
    }

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
        return JsonLogEntry.logEntryToJson(DeltaConst.VERSION_INIT, null, null);
    }

    private static JsonObject stateToJson(long version, Id patch, Id prev) {
        FmtLog.debug(LOG, "stateToJson ver=%d", version);
        return JsonLogEntry.logEntryToJson(version, patch, prev);
    }

    private void jsonSetState(JsonObject obj) {
        try {
            FmtLog.debug(LOG, "jsonToState %s",JSON.toStringFlat(obj));
            LogEntry entry = JsonLogEntry.jsonToLogEntry(obj);
            long ver = entry.getVersion().value();
            if ( ver == version )
                return ;
            Id newCurrent = entry.getPatchId();
            Id newPrevious = entry.getPrevious();
            newState(ver, newCurrent, newPrevious);
        } catch (RuntimeException ex) {
            FmtLog.error(this.getClass(), ex, "Failed to load the patch log index state");
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
        String pathVer = versionPath(ver);
        byte[] b = Zk.zkFetch(client, pathVer);
        if ( b == null )
            return null;
        Id id = Id.fromBytes(b);
        return id;
    }

    @Override
    public Version idToVersion(Id id) {
        String p = headerPath(id);
        JsonObject obj = Zk.zkFetchJson(client, p);
        LogEntry entry = JsonLogEntry.jsonToLogEntry(obj);
        return entry.getVersion();
    }

    @Override
    public LogEntry getPatchInfo(Id id) {
        String p = headerPath(id);
        JsonObject obj = Zk.zkFetchJson(client, p);
        LogEntry entry = JsonLogEntry.jsonToLogEntry(obj);
        return entry;
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

    @Override
    public void runWithLock(Runnable action) {
        synchronized(lock) {
            Zk.zkLock(zkLock, lockPath, ()->{
                syncVersionInfo();
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
            return Zk.zkLockRtn(zkLock, lockPath, ()->{
                syncVersionInfo();
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


    // Token not zk cluster wide and not checked.
    // Be trusting of client behaviour.
    // (The ownership token is used to check the client works properly.)

    // Timeouts:
    // Need to write!
    private static String jTimestamp = "timestamp";
    private static String jLockId =    "lockid";
    private static String jTicks =     "ticks";

    @Override
    public Id acquireLock() {
        // And createSet

        // Short or long term lock?
        Id lockToken =
            Zk.zkLockRtn(zkLock, lockPath, ()->{
                LockState lockState = readLock();
                if ( ! LockState.isFree(lockState) )
                    // Not free.
                    return null;

                Id lockTokenAlloc = Id.create();
                writeLockState(lockTokenAlloc, 1);
                return lockTokenAlloc;
            });
        return lockToken;
    }

    @Override
    public boolean refreshLock(Id session) {
        return Zk.zkLockRtn(zkLock, lockPath, ()->refreshLock$(session));
    }

    private boolean refreshLock$(Id session) {
        LockState lockState = readLock();
        if ( LockState.isFree(lockState) )
            // Free
            return false;
        if ( ! session.equals(lockState.session) )
            return false;
        writeLockState(session, lockState.ticks+1);
        return true;
    }

    private void writeLockState(Id session, long ticks) {
        JsonObject value = JSON.buildObject(builder->{
            builder.pair(jTimestamp, DateTimeUtils.nowAsXSDDateTimeString());
            builder.pair(jLockId, session.asPlainString());
            builder.pair(jTicks, ticks);
        });
        Zk.zkSetJson(client, lockStatePath, value);
    }

    @Override
    public LockState readLock() {
        JsonObject value = Zk.zkFetchJson(client, lockStatePath);
        if ( value == null || value.isEmpty() )
            return LockState.UNLOCKED;
        String dt = value.getString(jTimestamp);
        // Validate?
        String lockId = value.getString(jLockId);
        if ( dt == null || lockId == null ) {} // XXX
        long ticks = value.get(jTicks).getAsNumber().value().longValue();
        LockState lockState = LockState.create(Id.fromString(lockId), ticks);
        return lockState;
    }

    @Override
    public Id grabLock(Id oldSession) {
        return Zk.zkLockRtn(zkLock, lockPath, ()->{
            LockState lockState = readLock();
//            if ( ! LockState.isFree(lockState) && ! oldSession.equals(lockState.session) )
//                return null;
          if (  LockState.isFree(lockState) )
              return null;
          if ( ! oldSession.equals(lockState.session) )
              return null;

            // DRY with acquire
            Id lockTokenAlloc = Id.create();
            writeLockState(lockTokenAlloc, 1);
            return lockTokenAlloc;
        });
    }

    @Override
    public void releaseLock(Id session) {
        Objects.requireNonNull(session);
        Zk.zkLock(zkLock, lockPath, ()->{
            LockState lockState = readLock();
            if ( LockState.isFree(lockState) )
                return;
            if ( ! session.equals(lockState.session) )
                return;
            Zk.zkSet(client, lockStatePath, null);
        });
    }
}
