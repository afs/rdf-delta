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

import java.util.stream.Stream;

import org.apache.jena.atlas.lib.NotImplemented;
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
    // Sync lock.
    private Object lock = new Object();
    
    private final DataSourceDescription dsd;
    private final Id logId;
    private final PatchLogIndex logState;
    private final PatchStore patchStore;

    private final PatchStorage patchStorage;
    
    private Id earliestId; 
    private long earliestVersion;
    
    // Use one-way linked list from latest to earliest.
    // it is a cache of the patch log details.   
    // May be truncated - the earliest entry points to a patch not in the list - need to get from the storage an look fo rthe previous patch.

    public PatchLogBase(DataSourceDescription dsd,
                        PatchLogIndex logState,
                        PatchStorage patchStorage,
                        PatchStore patchStore) {
        this.dsd = dsd;
        // Currently, the log id is the id of the DataSource.
        this.logId = dsd.getId();
        
        this.logState = logState;
        this.patchStorage = patchStorage;
        this.patchStore = patchStore;
        // Initialize as "no patches".
        earliestId = logState.getEarliestId();
        earliestVersion = logState.isEmpty() ? DeltaConst.VERSION_INIT : DeltaConst.VERSION_FIRST;
        initFromStorage();
    }
    
    // Set earliestId, earliestVersion
    private void initFromStorage() {
        long latest = logState.getCurrentVersion();
        Id latestId = logState.getCurrentId();
    }
    
    @Override
    public Id getEarliestId() {
        return earliestId;
    }

    @Override
    public long getEarliestVersion() {
        return earliestVersion;
    }

    @Override
    public Id getLatestId() {
        if ( logState.isEmpty() )
            return getEarliestId();
        return logState.getCurrentId();
    }

    @Override
    public long getLatestVersion() {
        if ( logState.isEmpty() )
            return getEarliestVersion();
        return logState.getCurrentVersion();
    }
    
    @Override
    public PatchLogInfo getInfo() {
        if ( ! logState.isEmpty() ) {
            return new PatchLogInfo(dsd, getEarliestVersion(), getLatestVersion(), getLatestId());
        } else
            // Correct?
            return new PatchLogInfo(dsd, getEarliestVersion(), getEarliestVersion(), null);
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
        return earliestId == null;
    }

    @Override
    public PatchStore getPatchStore() {
        return patchStore;
    }

    @Override
    public boolean contains(Id patchId) {
        return false;
    }

    @Override
    public long append(RDFPatch patch) {
        synchronized(lock) {
            long version = logState.nextVersion();
            Id thisId = Id.fromNode(patch.getId());
            Id prevId = Id.fromNode(patch.getPrevious());        

            PatchValidation.validateNewPatch(this, thisId, prevId, PatchValidation::badPatchEx);

            patchStorage.store(thisId, patch);
            
            // This is the commit point.
            logState.save(version, thisId, prevId);
            
            if ( earliestId == null ) {
                earliestVersion = version;
                earliestId = thisId;
            }
            return version;
        }
    }

    @Override
    public RDFPatch fetch(Id patchId) {
        return patchStorage.fetch(patchId);
    }

    @Override
    public RDFPatch fetch(long version) {
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
    public Stream<RDFPatch> range(long start, long finish) {
        // Increment and probe.
        throw new NotImplemented();
        //return null;
    }

    @Override
    public Id find(long version) {
        return logState.mapVersionToId(version);
    }

    @Override
    public long find(Id id) {
        // Hmm
        return 0;
    }

    @Override
    public void release() {}

}
