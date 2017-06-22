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

package org.seaborne.delta.server.local;

import java.util.Objects ;
import java.util.stream.Stream;

import org.seaborne.delta.Id ;
import org.seaborne.delta.PatchLogInfo ;
import org.seaborne.patch.PatchHeader ;
import org.seaborne.patch.RDFPatch;

/** A sequence of patches for an {@link DataSource}.
 * All patches have a fixed id. In addition, a patch log
 * gives each patch a version number (sequence number).
 * <p><i>Version numbers</i>
 * <p>   
 * The next patch after patch with version N is found by trying to fetch the patch with version N+1.
 * Calling code must also check this is not null - versions may jump by more than one.
 * The gap is not expected to be big, nor frequent.
 * <p>
 * Versions numbers are not guaranteed to be stable across process restart.
 * Clients should use {@link #getDescription()} to find the current range. 
 */
public interface IPatchLog {
    
    /** Return the {@link Id} of first/earliest patch */
    public Id getEarliestId();
    
    /** Return the version of first/earliest patch */
    public long getEarliestVersion();

    /** Return the {@link Id} of most recent patch */
    public Id getLatestId();
    
    /** Return the version of most recent patch */
    public long getLatestVersion();

    /**
     * Return a description of the {@code IPatchLog}. This is the state at a
     * point in time and does not track subsequent changes to the patch log. In
     * other words, it is not "live".
     */
    public PatchLogInfo getDescription();

    /** Is the {@code IPatchLog} empty? */
    public boolean isEmpty();
    
    /** Does the log contain the patch with id {@code patchId}? */
    public boolean contains(Id patchId);

    /** Add a patch to the {@code PatchLog}. Return the version number. */
    public long append(RDFPatch patch);
    
    /** Get a patch by {@code Id}. */
    public RDFPatch fetch(Id patchId);
    
    /** Get a patch by version (version number may change across restarts). */
    public RDFPatch fetch(long version) ;

    /** Get patches by range - start/finish are inclusive */
    public Stream<RDFPatch> range(Id start, Id finish) ;

    /** Get patches by range - start/finish are inclusive */
    public Stream<RDFPatch> range(long start, long finish) ;

    /** Get a {@link PatchHeader} by {@code Id}.*/
    public default PatchHeader fetchHeader(Id patchId) {
        RDFPatch p = fetch(patchId) ;
        return p != null ? p.header() : null; 
    }
    
    /** Get a {@link PatchHeader} by version (version number may change across restarts) .*/
    public default PatchHeader fetchHeader(long version) {
        RDFPatch p = fetch(version) ;
        return p != null ? p.header() : null;
    }

    /* Get patch headers by range - start/finish are inclusive */
    public default Stream<PatchHeader> rangeHeaders(Id start, Id finish) {
        return range(start, finish).filter(Objects::nonNull).map(RDFPatch::header);
    }

    /* Get patch headers by range - start/finish are inclusive */
    public default Stream<PatchHeader> rangeHeaders(long start, long finish) {
        return range(start, finish).filter(Objects::nonNull).map(RDFPatch::header);
    }

    /** Translate a version number into its stable patch id. */
    public Id find(long version);
    
    /** Translate a patch id to version. */ 
    public long find(Id id);
}
