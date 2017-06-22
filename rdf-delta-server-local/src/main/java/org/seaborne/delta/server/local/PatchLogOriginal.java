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

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.* ;
import java.util.concurrent.ConcurrentHashMap ;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.InternalErrorException ;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.ext.com.google.common.collect.BiMap;
import org.apache.jena.ext.com.google.common.collect.HashBiMap;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.* ;
import org.seaborne.delta.lib.IOX;
import org.seaborne.patch.PatchHeader;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A sequence of patches for an {@link DataSource}. */

// OLD version, not up to date. 

public class PatchLogOriginal {
    // Terminology: "index" and "version"
    //   version number if the short form so be consistent.
    
    // Centralized logger for regular lifecyle reporting.
    private static Logger  LOG     = LoggerFactory.getLogger(PatchLogOriginal.class);
    // HISTORY
    // The history list is immutable except for the last (most recent) entry
    // which is updated when new entries arrive.
    // Is just a list of Id's enough if we have version->id. (versions are not contiguous). 
    
    static class HistoryEntry {
        final Id    prev;        // == patch.getPrevious(), except there may not be a patch object.
              Id    next;        // == fwd pointer in the list to next patch. 
        final PatchHeader patch; // Remove : replace with header.
        final int   version;
        final Id id;

        HistoryEntry(PatchHeader patch, int version, Id prev, Id thisId, Id next) {
            this.patch = patch;
            this.prev = prev;
            this.id = thisId;
            this.next = next;
            this.version = version; 
        }

        @Override
        public String toString() {
            return String.format("History: Version=%d, Id=%s, Next=%s, Prev=%s", version, id, next, prev);
        }
    }
    
    //Id of the DataSource 
    private final Id              dsRef;
    private final String          name;
    private final FileStore       fileStore;
    
//    private final PatchCache      patches = PatchCache.get();

    // XXX Reorganise datastructures. 
    private HistoryEntry          start = null;
    private HistoryEntry          finish = null;
    private Map<Id, HistoryEntry> historyEntries = new ConcurrentHashMap<>();
    // Two way id <-> version mampong of patch for this PatchLog
    private final BiMap<Id, Integer> idToNumber;
    private static boolean VALIDATE_PATCH_LOG = true;
    
    public static PatchLogOriginal attach(Id dsRef, String name, Location location) {
        FileStore fileStore = FileStore.attach(location, "patch");
        BiMap<Id, Integer> idToNumber = HashBiMap.create();
        // Read headers.
        // Builds the id to version index.
        fileStore.getIndexes().forEach(idx->{
            Path fn = fileStore.filename(idx);
            Id prev = null;
            HistoryEntry startEntry = null;
            HistoryEntry lastEntry = null;
            try ( InputStream in = new BufferedInputStream(Files.newInputStream(fn)) ) {
                PatchHeader header = RDFPatchOps.readHeader(in);
                
                if ( header.getId() == null )
                    LOG.warn("No id: "+fn);
                if ( header.getPrevious() == null && lastEntry != null )
                    LOG.warn("No previous id: "+fn);
                
                Id patchId = Id.fromNode(header.getId());
                Id previousId = Id.fromNode(header.getPrevious());
                if ( lastEntry != null ) {
                    if ( Objects.equals(lastEntry.id, previousId) )
                        FmtLog.warn(LOG, "Patch id=%s (previous=%s) does not refer to previous patch (id=%s)", patchId, previousId, lastEntry.id);
                    lastEntry.next = patchId;
                }
                HistoryEntry entry = new HistoryEntry(null, idx, previousId, patchId, prev);
                prev = patchId;
                lastEntry = entry;
                if ( startEntry == null )
                    startEntry = entry;
                idToNumber.put(patchId, idx);
                FmtLog.info(LOG, "Patch id=%s previous=%s version=%d", patchId, previousId, idx);
            } catch (NoSuchFileException | FileNotFoundException ex) {
                FmtLog.error(LOG, "No such file: "+fn);
                throw IOX.exception(ex);
            } catch ( IOException ex ) { throw IOX.exception(ex); }
        });
        
        // XXX Fill cache.
        fileStore.getIndexes().forEach(idx->{
            Path fn = fileStore.filename(idx);
            try ( InputStream in = new BufferedInputStream(Files.newInputStream(fn)) ) {
                RDFPatch patch = RDFPatchOps.read(in);
                if ( patch.getId() == null ) {
                    // Skip - already logged above. 
                    //FmtLog.warn(LOG, "Bad patch: no ID : %s", fn);
                    return;
                }
                //if ( patch.getPrevious() == null) {}
                Id patchId = Id.fromNode(patch.getId());
                PatchCache.get().put(patchId, patch);
            } catch ( IOException ex ) { throw IOX.exception(ex); } 
        });
        
        // Validate the patch chain.
        // Find the last patch id.
        if ( VALIDATE_PATCH_LOG ) {
            // read all patches.
            // validate
            ;
        }
        
        HistoryEntry currentEntry;
        if ( fileStore.isEmpty() ) {
            currentEntry = null;
            FmtLog.info(LOG, "PatchLog data id=%s starts empty", dsRef);
        } else {
            // Last patch starts history.
            int x = fileStore.getCurrentIndex();
            RDFPatch patch = fetch(fileStore, x);
            Id patchId = Id.fromNode(patch.getId());
            
            currentEntry = new HistoryEntry(patch.header(), x, null, patchId, null);
            FmtLog.info(LOG, "Patch log data id=%s ends (%s, version=%d)", dsRef, patchId, x);
        }
        return new PatchLogOriginal(dsRef, name, location, idToNumber, currentEntry);
    }

    private PatchLogOriginal(Id dsRef, String name, Location location, BiMap<Id, Integer> idToNumber, HistoryEntry startEntry) {
        this.dsRef = dsRef;
        this.name = name;
        this.idToNumber = idToNumber;
        // Linked list of one.
        this.start = startEntry;
        this.finish = startEntry;
        this.fileStore = FileStore.attach(location, "patch");
    }

    public Id getEarliestId() {
        if ( start == null )
            return null;
        return start.id;
    }

    public int getEarliestVersion() {
        int x = fileStore.getMinIndex();
        
        if ( start == null )
            return DeltaConst.VERSION_UNSET;
        return start.version;
    }

    public Id getLatestId() {
        if ( finish == null )
            return null;
        return finish.id;
    }

    public int getLatestVersion() {
        int x = fileStore.getCurrentIndex();
        
        if ( finish == null )
            return fileStore.getCurrentIndex();
        return finish.version;
    }

    public PatchLogInfo getDescription() {
        return new PatchLogInfo(dsRef, name, getEarliestVersion(), getLatestVersion(), getLatestId());
    }

    public boolean isEmpty() {
        if (start == null && finish == null )
            return true;
        if (start != null && finish != null )
            return false;
        FmtLog.warn(LOG, "Inconsistent: start %s, finish %s", start, finish);
        return false;
    }
    
    private void checkConsistent() {
        boolean b1 = fileStore.isEmpty();
        boolean b2 = (finish == null);
        if ( b1 != b2 ) {
            FmtLog.error(LOG, "Inconsistent: fileStore.isEmpty=%s : history empty=%s", b1, b2);
            if ( ! b1 )
                FmtLog.error(LOG, "    fileStore [%d, %d]", fileStore.getMinIndex(), fileStore.getCurrentIndex());
            if ( ! b2 )
                FmtLog.error(LOG, "    start %s, finish %s", start, finish);
            throw new DeltaException("PatchLog: "+name);  
        }
    }

    /** Validate a patch for this {@code PatchLog} */
    public boolean validate(RDFPatch patch) {
        Id previousId = Id.fromNode(patch.getPrevious());
        Id patchId = Id.fromNode(patch.getId());
        if ( previousId == null ) {
            if ( !isEmpty() ) {
                FmtLog.warn(LOG, "No previous for patch when PatchLog is not empty: patch=%s", patchId);
                return false;
            }
        } else {
            if ( isEmpty() ) {
                FmtLog.warn(LOG, "Previous reference for patch but PatchLog is empty: patch=%s : previous=%s", patchId, previousId);
                return false ;
            }
        }
        return true;
    }
    
    private void validateEx(Id patchId, Id previousId) {
        if ( patchId == null )
            badPatchEx("No id");
        
        // Check id is new.
        if ( historyEntries.containsKey(patchId) )
            badPatchEx("Patch already in the log: patch=%s", patchId);
        
        if ( isEmpty() ) {
            if ( previousId != null )
                badPatchEx("Previous reference for patch but PatchLog is empty: patch=%s : previous=%s", patchId, previousId);
            return ;
        }

        // Non-empty
        if ( previousId == null )
            badPatchEx("No previous for patch when PatchLog is non-empty: patch=%s", patchId);
        
        if ( patchId.equals(previousId) )
            badPatchEx("Patch id and patch previous are the same : patch=%s", patchId);
        
        // Check id is new.
        if ( historyEntries.containsKey(patchId) )
            badPatchEx("Duplicate patch: patch=%s, previous=%s", patchId, previousId);
        
        // Currently invalid check - the history is only from the last entry at startup forwards.
//        // Check previous exists
//        if ( ! historyEntries.containsKey(previousId) ) {
//            badPatchEx("Previous patch not in log: patch=%s, previous=%s", patchId, previousId);
//        }

        // and is latest (i.e. log head).
        if ( ! previousId.equals(getLatestId()) ) {
            // No empty log, previousId != null but does not match log head.
            // Validation should have caught this. 
            badPatchEx("Patch not an update on the latest logged one: id=%s prev=%s (log=[%d, %s])", 
                        patchId, previousId, getLatestVersion(), getLatestId());
        }
    }
    
    private void badPatchEx(String fmt, Object...args) {
        String msg = String.format(fmt, args);
        FmtLog.warn(LOG, msg); 
        throw new DeltaBadPatchException(msg);
    }
    
    private void badPatchWarn(String fmt, Object...args) {
        FmtLog.warn(LOG, String.format(fmt, args)); 
    }

    private void validateVersionEx(int version) {
        if ( idToNumber.inverse().containsKey(version) )
            // Internal consistency error. FleStore was supposed to make it unique.
            throw new InternalErrorException("Version already in-use: "+version);
    }
    
    private boolean validateEntry(RDFPatch patch, Id patchId, Id previousId) {
//        Id previousId = Id.fromNode(patch.getPrevious());
//        Id patchId = Id.fromNode(patch.getId());
        HistoryEntry entry = findHistoryEntry(patchId);
        if ( entry != null ) {
            Integer ver = idToNumber.get(patchId);
            if ( ver == null )
                FmtLog.warn(LOG, "Patch not registered: patch=%s (entry version=%d)", patchId, entry.version);
            else if ( ver != entry.version )
                FmtLog.warn(LOG, "Patch version=%d, but entry version=%d : %s", ver, entry.version, patchId);
            if ( previousId == null || previousId.isNil() ) {
                if ( entry.prev != null && ! entry.prev.isNil() )
                    FmtLog.warn(LOG, "Patch has a previous link, entry does not : %s (previous=%s)", patchId, previousId);
            } else {
                if ( entry.prev == null || entry.prev.isNil() )
                    FmtLog.warn(LOG, "Patch has no previous link, but entry does : %s (entry.prev=%s)", patchId, entry.prev);
            }
        } else {
            // No entry.
            if ( previousId != null && ! previousId.isNil() ) {
                FmtLog.warn(LOG, "Patch has a previous link, but there is no entry for this patch: %s (previous=%s)", patchId, previousId);
            }
        }
        
        return true;
    }
    
    /**
     * Add a patch to the PatchLog.
     * This operation does not store the patch; 
     * it is expected to be already persisted.
     * Only the {@code PatchLog} in-memory metadata is updated.
     * @param patch
     * @param version -- as decided by the filestore.
     */
    /*pakcage*/ void append(RDFPatch patch, int version) {
        // [DP-Fix]
        // If the patch is bad, we need to remove it else it will be assilated on restart.
        // Timing hole.
        
        Id patchId = Id.fromNode(patch.getId());
        Id previousId = Id.fromNode(patch.getPrevious());

        FmtLog.info(LOG, "Append: id=%s prev=%s to log %s", patchId, previousId, getDescription());
        
//        // [DP-Fake]
//        if ( finish != null && previousId == null ) {
//            previousId = finish.id;
//            // then it breaks in addHistoryEntry
//        }

        validateEx(patchId, previousId);
        validateVersionEx(version);
        
        FmtLog.info(LOG, "Patch id=%s", patchId.asString());
        FmtLog.info(LOG, "Patch id=%s previous=%s version=%d", patchId, previousId, version);
        
        HistoryEntry e = new HistoryEntry(patch.header(), version, previousId, patchId, null);
        addHistoryEntry(e);
        idToNumber.put(patchId, version);
        validateEntry(patch, patchId, previousId);
    }

    
    synchronized private void addHistoryEntry(HistoryEntry e) {
        PatchHeader patch = e.patch;
        Id id = Id.fromNode(patch.getId());
        Id previousId = Id.fromNode(patch.getPrevious());
        if ( start == null ) {
            start = e;
            // start.prev == null?
            finish = e;
            putHistoryEntry(id, e);
            FmtLog.info(LOG, "Patch starts history: id=%s", patch);
        } else {
            if ( previousId != null ) {
                if ( patch.getPrevious().equals(finish.patch.getId()) ) {
                    finish.next = id;
                    finish = e;
                    putHistoryEntry(id, e);
                    // if ( Objects.equals(currentHead(), patch.getPrevious()) ) {
                    FmtLog.info(LOG, "Patch added to history: id=%s", patch.getId());
                } else {
                    // No empty log, previousId != null but does not match log head.
                    // Validation should have caught this. 
                    FmtLog.warn(LOG, "Patch not added to the history: id=%s ", patch.getId());
                    throw new DeltaException("Patch not added to the history");
                }
            } else {
                FmtLog.warn(LOG, "No previous id but start!=null: Patch not added to the history: id=%s ", patch.getId());
                // No previousId.
                
            }
        }
    }

    private void putHistoryEntry(Id id, HistoryEntry e) {
        historyEntries.put(id, e);
    }
    
    /**
     * Get, as a copy, a slice of the history from the start point until the latest patch.
     */
    private List<RDFPatch> getPatchesFromHistory(Id start) {
        return getPatchesFromHistory(start, null);
    }

    /**
     * Get, as a copy, a slice of the history. start and finish as inclusive. finish may
     * be null meaning, "until latest"
     */
    synchronized private List<RDFPatch> getPatchesFromHistory(Id startPt, Id finish) {
        HistoryEntry e = 
            startPt==null ? this.start : findHistoryEntry(startPt);
        
        List<RDFPatch> x = new ArrayList<>();
        while (e != null) {
            RDFPatch patch = fetch(e.id);
            if ( patch == null ) {
                if ( x.size() > 0 ) {
                    List<String> sofar = x.stream().map(p->p.getId().toString()).collect(Collectors.toList());
                    FmtLog.error(LOG,  "No patch for id=%s (broken history after %d entries: %s)", e.id, sofar.size(), sofar);
                } else
                    FmtLog.error(LOG,  "No patch for id=%s", e.id);
                return null;
            }
            x.add(patch);
            if ( finish != null && e.patch.getId().equals(finish) )
                break;
            if ( e.next == null )
                e = null;
            else
                e = findHistoryEntry(e.next);
        }
        return x;
    }

    private HistoryEntry findHistoryEntry(Id start) {
        return historyEntries.get(start);
    }

    private void dumpHistory() {
        System.out.println("Map - "+historyEntries);
        HistoryEntry e = this.start;
        while(e!= null) {
            System.out.println("-- "+e);
            if ( e.next == null )
                break;
            e = historyEntries.get(e.next);
        }
    }
    
    public Stream<RDFPatch> range(Id start, Id finish) {
        return getPatchesFromHistory(start, finish).stream();
    }

    public FileStore getFileStore() {
        return fileStore;
    }

    private Id currentHead() {
        if ( finish == null )
            return null;
        return Id.fromNode(finish.patch.getId());
    }

    public boolean contains(Id patchId) {
        return idToNumber.containsKey(patchId) ;
    }

    public RDFPatch fetch(Id patchId) {
        Integer version = idToNumber.get(patchId);
        if ( version == null )
            return null;
        return fetch(version) ;
    }

    public RDFPatch fetch(int version) {
        return fetch(fileStore, version); 
    }
    
    // XXX PatchCache.
    private static RDFPatch fetch(FileStore fileStore, int version) {
        Path p = fileStore.filename(version);
        try (InputStream in = Files.newInputStream(p)) {
            RDFPatch patch = RDFPatchOps.read(in) ;
            return patch;
        }
        // FileNotFoundException - old style,included many error situations.
        // NIO -> NoSuchFileException
        catch ( NoSuchFileException | FileNotFoundException ex)
        { return null; }
        catch (IOException ex) { 
            throw IOX.exception(ex); 
        }
    }

    public Id find(int version) {
        return idToNumber.inverse().get(version);
    }
    
    @Override
    public String toString() {
        return String.format("PatchLog [%s, ver=%d head=%s]", name, getLatestVersion(), getLatestId());
    }
}
