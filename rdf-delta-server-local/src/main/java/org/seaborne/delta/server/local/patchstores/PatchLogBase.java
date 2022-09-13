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

package org.seaborne.delta.server.local.patchstores;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.lib.NotImplemented;
import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.*;
import org.seaborne.delta.server.local.PatchLog;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchValidation;
import org.seaborne.patch.RDFPatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of {@link PatchLog} using two components: a {@link PatchLogIndex} for the control record of the latest state
 * and a {@link PatchStorage} for the patches themselves.
 * <p>
 * The {@code PatchStorage} is updated first so may be ahead of the official log state.
 * <p>
 * ... except for eventually consistent stores, where the viewed {@code PatchStorage} may be behind,
 * in which case the only alternative is to poll/wait for the patch to show up.
 */

public class PatchLogBase implements PatchLog {
    private final static Logger LOG = LoggerFactory.getLogger(PatchLogBase.class);

    private Object lock = new Object();

    private final DataSourceDescription dsd;
    private final Id logId;
    private final PatchLogIndex logIndex;
    private final PatchStorage patchStorage;
    private final PatchStore patchStore;

    // Use one-way linked list from latest to earliest.
    // it is a cache of the patch log details.
    // May be truncated - the earliest entry points to a patch not in the list - need to
    // get from the storage an look for the previous patch.

    public PatchLogBase(DataSourceDescription dsd,
                        PatchLogIndex logIndex,
                        PatchStorage patchStorage,
                        PatchStore patchStore) {
        this.dsd = dsd;
        // Currently, the log id is the id of the DataSource.
        this.logId = dsd.getId();

        this.logIndex = logIndex;
        this.patchStorage = patchStorage;
        this.patchStore = patchStore;
        initFromStorage();
    }

    // Set earliestId, earliestVersion
    private void initFromStorage() {
        Version latest = logIndex.getCurrentVersion();
        Id latestId = logIndex.getCurrentId();
    }

    @Override
    public Id getEarliestId() {
        return logIndex.getEarliestId();
    }

    @Override
    public Version getEarliestVersion() {
        return logIndex.getEarliestVersion();
    }

    @Override
    public Id getLatestId() {
        if ( logIndex.isEmpty() )
            return getEarliestId();
        return logIndex.getCurrentId();
    }

    @Override
    public Version getLatestVersion() {
        if ( logIndex.isEmpty() )
            return getEarliestVersion();
        return logIndex.getCurrentVersion();
    }

    @Override
    public PatchLogInfo getInfo() {
        // Called when polling for changes during dataset sync.
        synchronized(lock) {
            logIndex.syncVersionInfo();
            return new PatchLogInfo(dsd, getEarliestVersion(), getLatestVersion(), getLatestId());
        }
    }

    @Override
    public DataSourceDescription getDescription() {
        return dsd;
    }

    @Override
    public Id getLogId() {
        return logId;
    }

    @Override
    public boolean isEmpty() {
        return logIndex.isEmpty();
    }

    @Override
    public PatchStore getPatchStore() {
        return patchStore;
    }

    public PatchLogIndex getPatchLogIndex() {
        return logIndex;
    }

    public PatchStorage getPatchLogStorage() {
        return patchStorage;
    }

    @Override
    public boolean contains(Id patchId) {
        return false;
    }

    @Override
    final
    public Version append(RDFPatch patch) {
//        System.err.println(">>append");
//        RDFPatchOps.write(System.err, patch);
//        System.err.println("<<append");

        return patchLogLockRtn(()->{
            Id thisId = Id.fromNode(patch.getId());
            Id prevId = Id.fromNode(patch.getPrevious());

            // Is it a reply of the last patch?
            if ( ! isEmpty() && getLatestId().equals(thisId) ) {
                if ( ! Objects.equals(prevId, logIndex.getPreviousId()) )
                    FmtLog.warn(LOG, "Patch id matches log head, but patch previous does not match log previous id");
                return getLatestVersion();
            }

            PatchValidation.validateNewPatch(this, thisId, prevId, PatchValidation::badPatchEx);

            // Commit. One or other of these must be the true "commit point.
            // We can inside the patchlog wide lock at this point.
            Version version = logIndex.nextVersion();

            patchStorage.store(version, thisId, patch);

            try {
                logIndex.save(version, thisId, prevId);
            } catch (Exception ex) {
                try {
                    patchStorage.delete(thisId);
                } catch (Exception deleteEx) {
                    FmtLog.error(LOG, ("Error occurred while attempting to delete patch file after failure to save log index info. patchId=" + thisId), deleteEx);
                }

                throw ex;
            }

            return version;
        });
    }

    protected void patchLogLock(Runnable action) {
        logIndex.runWithLock(action);
    }

    protected <X> X patchLogLockRtn(Supplier<X> action) {
        return logIndex.runWithLockRtn(action);
    }

    @Override
    public RDFPatch fetch(Id patchId) {
        return patchStorage.fetch(patchId);
    }

    @Override
    public RDFPatch fetch(Version version) {
        Id id = find(version);
        if ( id == null )
            return null;
        return fetch(id);
    }

    @Override
    public Stream<RDFPatch> range(Id start, Id finish) {
        // Assuming start < finish.
        throw new NotImplemented();
        //return null;
    }

    @Override
    public Stream<RDFPatch> range(Version start, Version finish) {
        // Brute force implementation.
        if ( start.isValid() && start.isAfter(logIndex.getCurrentVersion()) )
            throw new DeltaException(format("start out of range: range(%s,%s) but log is %s", start, finish, getInfo()));
        if ( finish.isValid() && finish.isBefore(logIndex.getEarliestVersion()) )
            throw new DeltaException(format("finish out of range: range(%s,%s) but log is %s", start, finish, getInfo()));
        if ( start.isAfter(finish) )
            throw new DeltaException(format("Range start after finish: range(%s,%s)", start, finish));
        List<RDFPatch> x = new ArrayList<>();
        // Range is inclusive.
        for ( long v = start.value() ; v <= finish.value() ; v++ ) {
            RDFPatch patch = fetch(Version.create(v));
            if ( patch != null )
                x.add(patch);
        }
        return x.stream();
    }

    @Override
    public Id find(Version version) {
        Id id = logIndex.versionToId(version);
        if ( id == null ) {
            logIndex.syncVersionInfo();
            id = logIndex.versionToId(version);
        }
        return id;
    }

    @Override
    public Version find(Id id) {
        FmtLog.warn(LOG, "Not implemented (yet). find(Id)");
        return Version.UNSET;
    }

    @Override
    public void delete() { }

    @Override
    public void releaseLog() { }

    @Override
    public Id acquireLock()                 { return logIndex.acquireLock(); }

    @Override
    public boolean refreshLock(Id session)  { return logIndex.refreshLock(session); }

    @Override
    public LockState readLock()             { return logIndex.readLock(); }

    @Override
    public Id grabLock(Id oldSession)       { return logIndex.grabLock(oldSession); }

    @Override
    public void releaseLock(Id session)     { logIndex.releaseLock(session); }

    @Override
    public String toString() {
        return "PatchLog: "+dsd;
    }
}
