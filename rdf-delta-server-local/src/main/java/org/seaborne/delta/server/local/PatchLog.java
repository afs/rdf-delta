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

package org.seaborne.delta.server.local;

import java.util.Objects ;
import java.util.stream.Stream;

import org.seaborne.delta.*;
import org.apache.jena.rdfpatch.PatchHeader ;
import org.apache.jena.rdfpatch.RDFPatch;

/** A {@code PatchLog} is the sequence of {@link RDFPatch RDFPatches} for one {@link DataSource}.
 * <p>
 * All patches have a fixed {@link Id}. In addition, a patch log
 * gives each patch a version number (sequence number).
 * <p><i>Version numbers</i>
 * <p>
 * The next patch after patch with version N is found by trying to fetch the patch with version N+1.
 * Calling code must also check this is not null - versions may jump by more than one.
 * The gap is not expected to be big, nor frequent.
 * <p>
 * Versions numbers are not guaranteed to be stable across process restart.
 * Clients should use {@link #getInfo()} to find the current range.
 */
public interface PatchLog {

    /** Return the {@link Id} of first/earliest patch */
    public Id getEarliestId();

    /** Return the version of first/earliest patch */
    public Version getEarliestVersion();

    /** Return the {@link Id} of most recent patch */
    public Id getLatestId();

    /** Return the version of most recent patch */
    public Version getLatestVersion();

    /**
     * Return a description of the current state of this {@code PatchLog}.
     * This is the state at a point in time and does not track subsequent
     * changes to the patch log. In other words, it is not "live".
     */
    public PatchLogInfo getInfo();

    /**
     * Return a description of the {@code PatchLog} - the basic information without
     * changing info like version number.
     */
    public DataSourceDescription getDescription();

    /**
     * Return the {@link PatchStore} responsible for this {@code PatchLog}.
     */
    public PatchStore getPatchStore();

    /**
     * Get the Id that identifies this log.
     */
    public Id getLogId();

    /** Is the {@code PatchLog} empty? */
    public boolean isEmpty();

    /** Does the log contain the patch with id {@code patchId}? */
    public boolean contains(Id patchId);

    /** Add a patch to the {@code PatchLog}. Return the version number. */
    public Version append(RDFPatch patch);

    /** Get a patch by {@code Id}. */
    public RDFPatch fetch(Id patchId);

    /** Get a patch by version (version number may change across restarts). */
    public RDFPatch fetch(Version version) ;

    /** Get patches by range - start/finish are inclusive */
    public Stream<RDFPatch> range(Id start, Id finish) ;

    /** Get patches by range - start/finish are inclusive */
    public Stream<RDFPatch> range(Version start, Version finish) ;

    /** Get a {@link PatchHeader} by {@code Id}.*/
    public default PatchHeader fetchHeader(Id patchId) {
        RDFPatch p = fetch(patchId) ;
        return p != null ? p.header() : null;
    }

    /** Get a {@link PatchHeader} by version (version number may change across restarts) .*/
    public default PatchHeader fetchHeader(Version version) {
        RDFPatch p = fetch(version) ;
        return p != null ? p.header() : null;
    }

    /* Get patch headers by range - start/finish are inclusive */
    public default Stream<PatchHeader> rangeHeaders(Id start, Id finish) {
        return range(start, finish).filter(Objects::nonNull).map(RDFPatch::header);
    }

    /* Get patch headers by range - start/finish are inclusive */
    public default Stream<PatchHeader> rangeHeaders(Version start, Version finish) {
        return range(start, finish).filter(Objects::nonNull).map(RDFPatch::header);
    }

    /** Translate a version number into its stable patch id. */
    public Id find(Version version);

    /** Translate a patch id to version. */
    public Version find(Id id);

    /** Delete - do not use again. */
    public void delete();

    /** Free in-process resources */
    public void releaseLog();

    /** Acquire the PatchLog mutex. */
    public Id acquireLock();

    /** Refresh the PatchLog mutex. */
    public boolean refreshLock(Id session);

    /**
     * Read the details of lock: the session/ownership id and the refresh index.
     * Return {@link LockState#UNLOCKED} if the lock is no longer held by anyone.
     */
    public LockState readLock();

    /**
     * Take the lock even if already held.
     * This changes the owner.
     * This resets the refresh index.
     *
     * Return new lock ownership/session Id or null if if the "oldSession" did
     * not match (someone else has grabbed the lock).
     */
    public Id grabLock(Id oldSession);

    /** Release the PatchLog mutex. */
    public void releaseLock(Id session);
}
