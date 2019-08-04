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

package org.seaborne.delta.server.local.patchstores;

import static java.lang.String.format;

import java.util.Objects;
import java.util.function.Supplier;

import org.seaborne.delta.DeltaException;
import org.seaborne.delta.Id;
import org.seaborne.delta.LogEntry;
import org.seaborne.delta.Version;

/** Implementation framework for a PatchLogIndex based on
 *  functions to id-&gt;version, version-&gt; and id->patchInfo.
 */
public abstract class PatchLogIndexBase implements PatchLogIndex {

    private final Object lock = new Object();
    // Natural values for a new patch log.
    private Version      earliestVersion = Version.INIT;
    private Id           earliestId      = null;
    private Version      currentVersion  = Version.INIT;
    private Id           currentId       = null;
    private Id           previousId      = null;

    protected PatchLogIndexBase(Version startVersion, Id startCurrent, Id startPrevious,
                                Version startEarliestVersion, Id startEarliestId ) {
        this.currentVersion = startVersion;
        this.currentId = startCurrent;
        this.previousId = startPrevious;
        this.earliestVersion = startEarliestVersion;
        this.earliestId = startEarliestId;
    }



    // Contract
    protected abstract Id fetchVersionToId(Version version);
    protected abstract Version genNextVersion();
    protected abstract Version fetchIdToVersion(Id id);
    protected abstract LogEntry fetchPatchInfo(Id id);
    // Two phase commit. Many systems will do all the work in "saveCommit".
    protected abstract void savePrepare(Version newVersion, Id newCurrent, Id newPrevious);
    protected abstract void saveCommit(Version newVersion, Id newCurrent, Id newPrevious);


    @Override
    final
    public Id versionToId(Version version) {
        if ( Objects.equals(currentVersion, version) )
            return currentId;
        return fetchVersionToId(version);
    }

    //    @Override
    //    public  Version nextVersion() {}


    @Override
    final
    public Version idToVersion(Id id) {
        if ( Objects.equals(currentId, id) )
            return currentVersion;
        return fetchPatchInfo(id).getVersion();
    }

    @Override
    final
    public LogEntry getPatchInfo(Id id) {
        if ( Objects.equals(currentId, id) )
            return new LogEntry(currentId, currentVersion, previousId);
        return fetchPatchInfo(id);
    }

    @Override
    final
    public Version nextVersion() {
        return genNextVersion();
    }

    @Override
    public void runWithLock(Runnable action) {
        synchronized (lock) {
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
    public boolean isEmpty() {
        //return version == DeltaConst.VERSION_UNSET || DeltaConst.VERSION_INIT;
        return currentId == null;
    }

//    @Override
//    public Version nextVersion() {
//        return null;
//    }

    @Override
    final public void save(Version newVersion, Id newCurrent, Id newPrevious) {
        Objects.requireNonNull(newVersion);
        Objects.requireNonNull(newCurrent);
        synchronized(lock) {
            if ( Objects.equals(newVersion, currentVersion) )
                throw new DeltaException(
                    format("save: Attempt save state with current version %s", currentVersion));
            if ( newVersion.isBefore(currentVersion) )
                throw new DeltaException(
                    format("save: Attempt save state at version %s with older version %s", currentVersion, newVersion));
            if ( ! Objects.equals(this.currentId, newPrevious) )
                throw new DeltaException(
                    format("save: Attempt save state when current != new prev (%s %s)", currentId, newPrevious));
            savePrepare(newVersion, newCurrent, newPrevious);
            saveCommit(newVersion, newCurrent, newPrevious);
            this.currentVersion = newVersion;
            this.currentId = newCurrent;
            this.previousId = newPrevious;
            if ( earliestId == null ) {
                earliestId = newCurrent;
                earliestVersion = newVersion;
            }
        }

    }

    @Override
    final public Version getEarliestVersion() {
        return earliestVersion;
    }

    @Override
    final public Id getEarliestId() {
        return earliestId;
    }

    @Override
    final public Version getCurrentVersion() {
        return currentVersion;
    }

    @Override
    final public Id getCurrentId() {
        return currentId;
    }

    @Override
    final public Id getPreviousId() {
        return previousId;
    }

    @Override
    public void release() {}

    @Override
    public void delete() {}

    @Override
    public void syncVersionInfo() {}

}
