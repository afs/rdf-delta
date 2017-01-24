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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.graph.Node;
import org.seaborne.delta.Id;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Collection of patches for on dataset */
public class PatchSet {
    // Centralized logger for regualr lifecyle reporting.
    private static Logger  LOG     = LoggerFactory.getLogger(PatchSet.class);
    // Tree?

    // All patches.
    // Need indirection? Patch[local]->RDFPatch
    private Map<Id, Patch> patches = new ConcurrentHashMap<>();

    // HISTORY
    // The history list is immutable except for the last (most recent) entry
    // which is updated to
    static class HistoryEntry {
        final Id    prev;        // == patch(next).getParent()
        Id          next;        // Not final! == null at end, and can be extended.
        final Patch patch;

        HistoryEntry(Patch patch, Id prev, Id next) {
            this.patch = patch;
            this.prev = prev;
            this.next = next;
        }

        @Override
        public String toString() {
            return String.format("History: Next=%s, Prev=%s", next, prev);
        }
    }

    //Id of the DataSource 
    private final Id              id;
    private final FileStore       fileStore;

    private HistoryEntry          start;
    private HistoryEntry          finish;
    private Map<Id, HistoryEntry> historyEntries = new ConcurrentHashMap<>();
    //private Map<Integer, Id>      versionToId = new ConcurrentHashMap<>();

    private List<PatchHandler>    handlers       = new ArrayList<>();


    public PatchSet(Id id, String location) {
        this.id = id;
        this.fileStore = FileStore.attach(location, "patch");
    }

    public PatchSetInfo getInfo() {
        return new PatchSetInfo(0L, 0L, id, null);
    }

    public void add(Patch patch) {
        // Validate.
        patches.put(patch.getIdAsId(), patch);
        HistoryEntry e = new HistoryEntry(patch, patch.getParentIdAsId(), null);
        addHistoryEntry(e);
    }

    synchronized private void addHistoryEntry(HistoryEntry e) {
        Patch patch = e.patch;
        Id id = Id.fromNode(patch.getId());
        Node parentId = patch.getParentId();
        FmtLog.info(LOG, "Patch id=%s (parent=%s)", id, parentId);
        patches.put(id, patch);
        if ( start == null ) {
            start = e;
            // start.prev == null?
            finish = e;
            historyEntries.put(id, e);
            FmtLog.info(LOG, "Patch starts history: id=%s", patch.getId());
        } else {
            
            if ( parentId != null ) {
                if ( patch.getParentId().equals(finish.patch.getId()) ) {
                    finish.next = id;
                    finish = e;
                    historyEntries.put(id, e);
                    // if ( Objects.equals(currentHead(), patch.getParent()) ) {
                    FmtLog.info(LOG, "Patch added to history: id=%s", patch.getId());
                } else {
                    FmtLog.warn(LOG, "Patch not added to the history: id=%s", patch.getId());
                }
            }
        }
    }

    /**
     * Get, as a copy, a slice of the history from the start point until the latest patch.
     */
    private List<Patch> getPatchesFromHistory(Id start) {
        return getPatchesFromHistory(start, null);
    }

    /**
     * Get, as a copy, a slice of the history. start and finish as inclusive. finish may
     * be null meaning, "until latest"
     */
    synchronized private List<Patch> getPatchesFromHistory(Id startPt, Id finish) {
        HistoryEntry e = 
            startPt==null ? this.start : findHistoryEntry(startPt);
        
        List<Patch> x = new ArrayList<>();
        while (e != null) {
            x.add(e.patch);
            if ( finish != null && e.patch.getId().equals(finish) )
                break;
            if ( e.next == null )
                e = null;
            else
                e = historyEntries.get(e.next);
        }
        return x;
    }

    private HistoryEntry findHistoryEntry(Id start) {
        return historyEntries.get(start);
    }

    public Stream<Patch> range(Id start, Id finish) {
        Patch p = patches.get(start);
        // "Next"

        System.err.println("Unfinished: PatchSet.range");
        return null;
    }

    public void addHandler(PatchHandler handler) {
        handlers.add(handler);
    }

    /**
     * Access to thr handler list - this can be manipulated but the the caller is
     * responsible for ensuring no patches are delivered or processed while being changed.
     * <p>
     * So safe to do during startup, not while live.
     * <p>
     * Low-level access : use with care.
     */
    public List<PatchHandler> getHandlers() {
        return handlers;
    }

    public FileStore getFileStore() {
        return fileStore;
    }

    private Id currentHead() {
        if ( finish == null )
            return null;
        return finish.patch.getIdAsId();
    }

//    public void processHistoryFrom(Id start, PatchHandler c) {
//        List<Patch> x = getPatchesFromHistory(start, null);
//        x.forEach((p) -> c.handle(p));
//    }

    public boolean contains(Id patchId) {
        return patches.containsKey(patchId) ;
    }

    public Patch fetch(Id patchId) {
        return patches.get(patchId) ;
    }

    public RDFPatch fetch(int version) {
        Path p = fileStore.filename(version);
        try {
            InputStream in = Files.newInputStream(p);
            RDFPatch patch = RDFPatchOps.read(in) ;
            return patch;
        } catch (IOException ex) {
            IO.exception(ex);
            return null;
        }
    }
    
    public Id find(int version) {
        // XXX Do better!
        Path p = fileStore.filename(version);
        try {
            InputStream in = Files.newInputStream(p);
            RDFPatch patch = RDFPatchOps.read(in) ;
            return Id.fromNode(patch.getId());
        } catch (IOException ex) {
            IO.exception(ex);
            return null;
        }
    }

    // Clear out.

}
