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

import org.apache.jena.atlas.lib.Cache;
import org.apache.jena.atlas.lib.CacheFactory;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.graph.Node;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.Id;
import org.seaborne.delta.lib.IOX;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Collection of patches for on dataset */
public class PatchLog {
    // Centralized logger for regualr lifecyle reporting.
    private static Logger  LOG     = LoggerFactory.getLogger(PatchLog.class);
    // Tree?

    // All patches.
    // Need indirection? Patch[local]->RDFPatch
    // --> LRU cache.
    private Map<Id, Patch> patches = new ConcurrentHashMap<>();
    
    // Global id->patch cache.
    private static Cache<Id, Patch> patchCache = CacheFactory.createCache(1000);
    static {
        patchCache.setDropHandler((id,patch)->LOG.info("Cache drop patch: "+id));
    }

    // HISTORY
    // The history list is immutable except for the last (most recent) entry
    // which is updated to
    static class HistoryEntry {
        final Id    prev;       // == patch(next).getParent()
              Id    next;       // Not final! == null at end, and can be extended.
        final Patch patch;      // Remove.
        final int   version;
        final Id id;

        HistoryEntry(Patch patch, int version, Id prev, Id thisId, Id next) {
            this.patch = patch;
            this.prev = prev;
            this.id = thisId;
            this.next = next;
            this.version = version; 
        }

        @Override
        public String toString() {
            return String.format("History: Version=%d, Id=%d, Next=%s, Prev=%s", version, id, next, prev);
        }
    }

    //Id of the DataSource 
    private final Id              dsRef;
    private final FileStore       fileStore;

    private HistoryEntry          start = null;
    private HistoryEntry          finish = null;
    private Map<Id, HistoryEntry> historyEntries = new ConcurrentHashMap<>();
    //private Map<Integer, Id>      versionToId = new ConcurrentHashMap<>();

    private List<PatchHandler>    handlers       = new ArrayList<>();

    private static boolean VALIDATE_PATCH_LOG = true;
    
    public static PatchLog attach(Id dsRef, Location location) {
        FileStore fileStore = FileStore.attach(location, "patch");

        if ( VALIDATE_PATCH_LOG ) {
            ;
        }
        
        final HistoryEntry currentEntry;
        if ( fileStore.isEmpty() ) {
            currentEntry = null;
            FmtLog.info(LOG, "PatchLog for %s, starts empty", dsRef);
        } else {
            int x = fileStore.getCurrentIndex();
            // Patch read meta only?
            // Why the "Patch" class for an History Entry?
            RDFPatch patch = fetch(fileStore, x);
            Id patchId = Id.fromNode(patch.getId());
            Patch holder = new Patch(patch, null, null);
            currentEntry = new HistoryEntry(holder, x, null, patchId, null);
            FmtLog.info(LOG, "PatchLog for %s, history until %s", dsRef, patchId); 
        }
        
        // Validate the patch chain.
        // Find the last patch id.
        
        
        return new PatchLog(dsRef, location, currentEntry);
    }

    private PatchLog(Id dsRef, Location location, HistoryEntry startEntry) {
        // XXX [DISK]
        this.dsRef = dsRef;
        // Linked list of one.
        this.start = startEntry;
        this.finish = startEntry;
        this.fileStore = FileStore.attach(location, "patch");
    }

    public Id getLatestId() {
        HistoryEntry e = finish;
        return e.id;
    }

    public int getLatestVersion() {
        HistoryEntry e = finish;
        return e.version;
    }

    public PatchLogInfo getInfo(boolean unimplemented) {
        // XXX PatchLogInfo
        return new PatchLogInfo(dsRef, -1, -1, null); 
    }
    
    public boolean isEmpty() {
        // 
        boolean b1 = fileStore.isEmpty();
        boolean b2 = finish == null;
        if ( b1 != b2 )
            FmtLog.warn(LOG, "Inconsistent: fileStore.isEmpty=%s : history empty=%s", b1, b2);
        return b2;
    }

    /** Validate a patch for this {@code PatchLog} */
    public boolean validate(Patch patch) {
        Id parent = patch.getParentIdAsId();
        Id patchId = patch.getIdAsId();
        if ( parent == null ) {
            if ( !isEmpty() ) {
                FmtLog.warn(LOG, "No parent for patch when PatchLog is not empty: patch=%s", patchId);
                return false;
            }
        } else {
            if ( isEmpty() ) {
                FmtLog.warn(LOG, "Parent for patch but PatchLog is empty: patch=%s : parent=%s", patchId, parent);
                return false ;
            } else {
                if ( ! parent.equals(finish.id) ) {
                    FmtLog.warn(LOG, "Parent for patch does not match PatchLog latest: patch=(%s  parent=%s) : latest=%s", patchId, parent, finish.id);
                    return false ;
                }
            }
        }
        return true;
    }
    
    /**
     * Add a patch to the PatchLog.
     * This operation does not store the patch; 
     * it is expected to be already persisted.
     * Only the {@code PatchLog} in-mmeory metadata is updated. 
     */
    void addMeta(Patch patch) {
        // Validate.
        validate(patch);
        patches.put(patch.getIdAsId(), patch);
        // XXX Version
        int version = -99;
        HistoryEntry e = new HistoryEntry(patch, version, patch.getParentIdAsId(), patch.getIdAsId(), null);
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

        System.err.println("Unfinished: PatchLog.range");
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
        return fetch(fileStore, version); 
    }
    
    private static RDFPatch fetch(FileStore fileStore, int version) {
        Path p = fileStore.filename(version);
        try {
            InputStream in = Files.newInputStream(p);
            RDFPatch patch = RDFPatchOps.read(in) ;
            return patch;
        } catch (IOException ex) { throw IOX.exception(ex); }
    }

    public Id find(int version) {
        // XXX Do better!
        Path p = fileStore.filename(version);
        try {
            InputStream in = Files.newInputStream(p);
            RDFPatch patch = RDFPatchOps.read(in) ;
            return Id.fromNode(patch.getId());
        } catch (IOException ex) { throw IOX.exception(ex); }
    }

    // Clear out.

}
