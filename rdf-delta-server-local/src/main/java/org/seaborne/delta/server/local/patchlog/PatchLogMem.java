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

package org.seaborne.delta.server.local.patchlog;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.Id;
import org.seaborne.patch.RDFPatch;

/** Simple, lightweight implementation of {@link PatchLog}. */ 
public class PatchLogMem extends AbstractPatchLog implements PatchLog {

    // Versions start at 1 :: Version x is at slot (x-1).
    private List<RDFPatch> patchesList = Collections.synchronizedList(new LinkedList<>());
    private Map<Id, RDFPatch> patchesMap = new ConcurrentHashMap<>(); 

    @Override
    protected synchronized RDFPatch earliestPatch() {
        return patchesList.isEmpty() ? null : patchesList.get(0);
    }

    @Override
    protected synchronized RDFPatch latestPatch() {
        return patchesList.isEmpty() ? null : patchesList.get(patchesList.size() - 1);
    }
    
    @Override
    protected synchronized long earliestVersion() {
        return patchesList.isEmpty() ? DeltaConst.VERSION_UNSET : DeltaConst.VERSION_FIRST; 
    }

    @Override
    protected synchronized long latestVersion() {
        return patchesList.isEmpty() ? DeltaConst.VERSION_UNSET : patchesList.size();
    }
    
    public PatchLogMem(DataSourceDescription dsd) {
        super(dsd);
    }

    @Override
    public boolean contains(Id patchId) {
        return patchesMap.containsKey(patchId);
    }

    @Override
    public synchronized long append(RDFPatch patch) {
        Id id = Id.fromNode(patch.getId());
        patchesMap.put(id, patch);
        patchesList.add(patch);
        return patchesList.size();
    }

    @Override
    public RDFPatch fetch(Id patchId) {
        return patchesMap.get(patchId);
    }

    private boolean validVersion(long version) {
        // Versions run from VERSION_INIT (no versions) to patchesList.size()
        return version > DeltaConst.VERSION_INIT && version <= patchesList.size();
    }
    
    private int slot(long version) {
        return (int)version-1;
    }
    

    @Override
    public RDFPatch fetch(long version) {
        if ( ! validVersion(version) )
            return null;
        return patchesList.get(slot(version));
    }

    @Override
    public Stream<RDFPatch> range(Id start, Id finish) {
        long x1 = find(start);
        if ( x1 < 0 )
            return null;
        long x2 = find(finish);
        if ( x2 < 0 )
            return null;
        return range(x1, x2);
    }

    @Override
    public Stream<RDFPatch> range(long start, long finish) {
        if ( ! validVersion(start) )
            return null;
        if ( ! validVersion(finish) )
            return null;
        // Version x at slot (x-1)
        return patchesList.subList(slot(start), slot(finish+1)).stream();
    }

    @Override
    public Id find(long version) {
        if ( ! validVersion(version) )
            return null;
        return Id.fromNode(patchesList.get(slot(version)).getId()); 
    }

    @Override
    public long find(Id id) {
        RDFPatch patch = patchesMap.get(id);
        if ( patch == null )
            return DeltaConst.VERSION_UNSET;
        int x = patchesList.indexOf(patch);
        if ( x < 0 )
            return DeltaConst.VERSION_UNSET;
        // Slot to version.
        return x+1;
    }

    @Override
    public void release() {}
}
