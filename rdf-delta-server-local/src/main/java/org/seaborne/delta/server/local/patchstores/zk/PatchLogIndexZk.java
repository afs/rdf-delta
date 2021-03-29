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

import java.util.*;
import java.util.function.Supplier;

import org.apache.curator.utils.ZKPaths;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.lib.DateTimeUtils;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.zookeeper.Watcher;
import org.seaborne.delta.*;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.server.local.JsonLogEntry;
import org.seaborne.delta.server.local.LogEntry;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.seaborne.delta.zk.UncheckedZkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** State control for a {@link PatchStore} */
public class PatchLogIndexZk implements PatchLogIndex {
    private static final Logger LOG = LoggerFactory.getLogger(PatchLogIndexZk.class);

    private final Object lock = new Object();
    private final UncheckedZkConnection zk;

    private final String logName;
    private final String statePath;
    private final String lockPath;
    private final String lockStatePath;
    private final String versionsPath;
    private final String headersPath;

    // null => no watching.
    //private Watcher logStateWatcher = null;
    private final Watcher logStateWatcher;

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
    public PatchLogIndexZk(UncheckedZkConnection client, String instance, DataSourceDescription dsd, String logPath) {
        // THis gets called twice in the creator - sees its own create via ZK watcher.
        this.zk = client;
        this.logName = dsd.getName();
        this.statePath      = ZKPaths.makePath(logPath, ZkConst.nState, new String[]{});
        this.lockPath       = ZKPaths.makePath(logPath, ZkConst.nLock, new String[]{});
        this.lockStatePath  = ZKPaths.makePath(logPath, ZkConst.nLockState, new String[]{});
        this.versionsPath   = ZKPaths.makePath(logPath, ZkConst.nVersions, new String[]{});
        this.headersPath    = ZKPaths.makePath(logPath, ZkConst.nHeaders, new String[]{});
        this.logStateWatcher = (event)->{
          synchronized(lock) {
              FmtLog.debug(LOG, "++ [%s:%s] Log watcher", instance, logName);
              syncState();
          }
        };

        // Find earliest.
        List<String> versions = client.fetchChildren(versionsPath);
        //Guess: 1
        if (versions.isEmpty())
            earliestVersion = Version.INIT;
        else if (versions.contains("00000001"))
            // Fast-track the "obvious" answer
            earliestVersion = Version.create(1);
        else {
            try {
                long ver = versions.stream().map(this::versionFromName).filter(v->(v>0)).min(Long::compare).get();
                earliestVersion = Version.create(ver);
            } catch (final NoSuchElementException ignored) {  }
        }
        earliestId = versionToId(earliestVersion);
        // Initialize, start watching
        stateOrInit();
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
            /* Keep header info - the (version, id, prev) is saved in /headers/<id> when a patch is stored in addition to the
             * /versions/NNNN which as just id.
             * This isn't necessary for operation.
             * It can be used to check the patch store.
             */
            this.zk.createAndSetZNode(headerPath(patch), bytes);
            // Write version->id mapping.
            this.zk.createAndSetZNode(versionPath(version), patch.asBytes());
        }
        this.zk.setZNode(statePath, bytes);
    }

    private void syncState() {
        JsonObject obj = getWatchedState();
        if ( obj != null )
            jsonSetState(obj);
    }

    private JsonObject getWatchedState() {
        return this.zk.fetchJson(logStateWatcher, statePath);
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
        byte[] b = this.zk.fetch(versionPath(ver));
        if ( b == null )
            return null;
        return Id.fromBytes(b);
    }

    @Override
    public Version idToVersion(Id id) {
        String p = headerPath(id);
        JsonObject obj = this.zk.fetchJson(p);
        LogEntry entry = JsonLogEntry.jsonToLogEntry(obj);
        return entry.getVersion();
    }

    @Override
    public LogEntry getPatchInfo(Id id) {
        String p = headerPath(id);
        JsonObject obj = this.zk.fetchJson(p);
        return JsonLogEntry.jsonToLogEntry(obj);
   }

    private String versionPath(Version ver) { return versionPath(ver.value()) ; }
    private String versionPath(long ver) { return ZKPaths.makePath(versionsPath, String.format("%08d", ver), new String[]{}); }
    private String headerPath(Id id) { return ZKPaths.makePath(headersPath, id.asPlainString(), new String[]{}); }

    private long versionFromName(String name) {
        try {
            return Long.parseLong(name);
        } catch (NumberFormatException ex) {
            Log.warn(this, "Attempt to extract the version from '"+name+"'");
            return -1;
        }
    }

    @Override
    public void runWithLock(final Runnable action) {
        this.runWithLock(
            () -> {
                action.run();
                return null;
            }
        );
    }

    @Override
    public <X> X runWithLock(Supplier<X> action) {
        synchronized(lock) {
            syncVersionInfo();
            return this.zk.runWithLock(this.lockPath, action);
        }
    }


    // Token not zk cluster wide and not checked.
    // Be trusting of client behaviour.
    // (The ownership token is used to check the client works properly.)

    // Timeouts:
    // Need to write!
    private static final String jTimestamp = "timestamp";
    private static final String jLockId =    "lockid";
    private static final String jTicks =     "ticks";

    @Override
    public Id acquireLock() {
        // And createSet

        // Short or long term lock?
        return this.runWithLock(
            ()-> {
                LockState lockState = readLock();
                if ( ! LockState.isFree(lockState) ) {
                    // Not free.
                    return null;
                }

                Id lockTokenAlloc = Id.create();
                writeLockState(lockTokenAlloc, 1);
                return lockTokenAlloc;
            }
        );
    }

    @Override
    public boolean refreshLock(Id session) {
        return this.runWithLock(()->refreshLock$(session));
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
        this.zk.setZNode(lockStatePath, value);
    }

    @Override
    public LockState readLock() {
        JsonObject value = this.zk.fetchJson(lockStatePath);
        if ( value == null || value.isEmpty() )
            return LockState.UNLOCKED;
        // Validate?
        String lockId = value.getString(jLockId);
        long ticks = value.get(jTicks).getAsNumber().value().longValue();
        return LockState.create(Id.fromString(lockId), ticks);
    }

    @Override
    public Id grabLock(Id oldSession) {
        return this.runWithLock(
            ()-> {
                LockState lockState = readLock();
                if (  LockState.isFree(lockState) )
                  return null;
                if ( ! oldSession.equals(lockState.session) )
                  return null;

                // DRY with acquire
                Id lockTokenAlloc = Id.create();
                this.writeLockState(lockTokenAlloc, 1);
                return lockTokenAlloc;
            }
        );
    }

    @Override
    public void releaseLock(Id session) {
        Objects.requireNonNull(session);
        this.runWithLock(
            ()-> {
                LockState lockState = readLock();
                if ( LockState.isFree(lockState) )
                    return;
                if ( ! session.equals(lockState.session) )
                    return;
                this.zk.setZNode(lockStatePath, new byte[0]);
            }
        );
    }
}
