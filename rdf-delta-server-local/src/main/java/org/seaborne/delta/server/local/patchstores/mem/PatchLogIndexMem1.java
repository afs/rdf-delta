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

package org.seaborne.delta.server.local.patchstores.mem;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.seaborne.delta.Id;
import org.seaborne.delta.Version;
import org.seaborne.delta.server.local.LogEntry;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;

/** State control for a {@link PatchStore} */
public class PatchLogIndexMem1 implements PatchLogIndex {
    private Object            lock            = new Object();
    private Map<Id, LogEntry> patchHeaders    = new ConcurrentHashMap<>();
    private Map<Version, Id>  versions        = new ConcurrentHashMap<>();

    private Version   earliestVersion = Version.UNSET;
    private Id        earliestId      = null;

    private Version   version         = Version.UNSET;
    private Id        current         = null;
    private Id        prev            = null;

    /*unused - has been replaced*/private PatchLogIndexMem1() {
        version = Version.INIT;
        earliestVersion = Version.INIT;
        current = null;
        prev = null;
    }

    @Override
    public boolean isEmpty() {
        //return version == DeltaConst.VERSION_UNSET || DeltaConst.VERSION_INIT;
        return current == null;
    }

    @Override
    public Version nextVersion() {
        return getCurrentVersion().inc();
    }

    @Override
    public void save(Version version, Id patch, Id prev) {
        this.version = version;
        this.current = patch;
        this.prev = prev;
        if ( earliestId == null ) {
            earliestVersion = version;
            earliestId = patch;
        }

        versions.put(version, patch);
        patchHeaders.put(patch, new LogEntry(current, version, prev));
    }

    @Override
    public Version getCurrentVersion() {
        return version;
    }

    @Override
    public Id getCurrentId() {
        return current;
    }

    @Override
    public Id getPreviousId() {
        return prev;
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
        return versions.get(ver);
    }

    @Override
    public Version idToVersion(Id id) {
        LogEntry info = getPatchInfo(id);
        if ( info == null )
            return null;
        return info.getVersion();
    }

    @Override
    public void release() {
        versions.clear();
        version = Version.UNSET;
        current = null;
        prev = null;
    }

    @Override
    public void delete() {
        release();
    }

    @Override
    public void runWithLock(Runnable action) {
        synchronized(lock) {
            action.run();
        }
    }

    @Override
    public <X> X runWithLockRtn(Supplier<X> action) {
        synchronized(lock) {
            return action.get();
        }
    }

    @Override
    public LogEntry getPatchInfo(Id id) {
        return patchHeaders.get(id);
    }

    @Override
    public void syncVersionInfo() {}
}
