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

import static org.seaborne.delta.DeltaConst.VERSION_INIT ;
import static org.seaborne.delta.DeltaConst.VERSION_UNSET ;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Iterator ;
import java.util.Map ;
import java.util.Objects ;
import java.util.concurrent.ConcurrentHashMap ;
import java.util.stream.LongStream ;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.InternalErrorException ;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.ext.com.google.common.collect.BiMap;
import org.apache.jena.ext.com.google.common.collect.HashBiMap;
import org.apache.jena.ext.com.google.common.collect.Maps ;
import org.apache.jena.graph.Node ;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.* ;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.server.local.DataSource ;
import org.seaborne.patch.PatchHeader;
import org.seaborne.patch.PatchReaderHeader ;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesWriter ;
import org.seaborne.riot.tio.TokenWriter ;
import org.seaborne.riot.tio.impl.TokenWriterText ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A sequence of patches for an {@link DataSource}. */
public class PatchLogFile implements PatchLog {
    private static final boolean CHECKING = true ;

    // Centralized logger for regular lifecyle reporting.
    private static Logger  LOG     = LoggerFactory.getLogger(PatchLogFile.class);

    //Id of the DataSource 
    private final Id              dsRef;
    private final String          name;
    private final FileStore       fileStore;

    // Forward, backwards chain?
    // c.g. HistoryEntry
    private BiMap<Id, Long> idToVersion =  Maps.synchronizedBiMap(HashBiMap.create()); 
    private Map<Id, PatchHeader> headers = new ConcurrentHashMap<>();
    
    private Id latestId = null;
    private long latestVersion = VERSION_UNSET;
    
    /** Attached to an existing {@code PatchLog}. */
    public static PatchLogFile attach(Id dsRef, String name, Location location) {
        return new PatchLogFile(dsRef, name, location);
    }
    
//    /** Create a new {@code PatchLog}. */
//    public static PatchLog create(Id dsRef, String name, Location location) {
//        return new PatchLog(dsRef, name, location, null);
//    }

    private PatchLogFile(Id dsRef, String name, Location location) {
        this.dsRef = dsRef;
        this.name = name;
        this.fileStore = FileStore.attach(location, "patch");
        initFromFileStore();
    }
    
    @Override
    public Id getLogId() {
        return dsRef;
    }
    
    private void initFromFileStore() {
        Iterator<Long> iter = fileStore.getIndexes().iterator();
        PatchHeader previous = null;
        for ( ; iter.hasNext() ; ) {
            long idx = iter.next();
            try ( InputStream in = fileStore.open(idx) ) {
                PatchHeader patchHeader = PatchReaderHeader.readerHeader(in);
                if ( patchHeader == null ) {
                    FmtLog.error(LOG, "Can't read header: idx=%d", idx);
                    continue;
                }
                Id id = Id.fromNode(patchHeader.getId());
                if ( id == null ) {
                    FmtLog.error(LOG, "Can't find id: idx=%d: id=%s", idx, id);
                    continue;
                }
                else {
                    if ( contains(id) ) {
                        FmtLog.error(LOG, "Duplicate: idx=%d: id=%s", idx, id);
                    }
                }
                    
                Id prev = Id.fromNode(patchHeader.getPrevious());
                if ( prev != null ) {
                    // We process entries in order so we should have seen previous by now.
                    if ( ! contains(prev) ) {
                        FmtLog.error(LOG, "Can't find previous: idx=%d: id=%s, prev=%s", idx, id, prev);
                        continue;
                    }
                }
                headers.put(id, patchHeader);
                idToVersion.put(id, Long.valueOf(idx));
                latestId = id;
                latestVersion = idx;
            }
            catch (NoSuchFileException ex) {}
            catch (IOException ex) {}
        }
    }

    @Override
    public Id getEarliestId() {
        long x = getEarliestVersion();
        if ( x <= 0 )
            // Legal versions start 1. 
            return null;
        return idToVersion.inverse().get(x);
    }

    @Override
    public long getEarliestVersion() {
        return fileStore.getMinIndex();
    }

    @Override
    public Id getLatestId() {
        validateLatest();
        return latestId;
    }

    @Override
    public long getLatestVersion() {
        validateLatest();
       return fileStore.getCurrentIndex();
    }

    private void validateLatest() {
        if ( CHECKING ) {
            long x = fileStore.getCurrentIndex();
            // latestVersion = -1 (UNSET) and getCurrentIndex==0 (INIT) is OK
            if ( latestVersion == VERSION_UNSET ) {
                if ( x != VERSION_INIT )
                    FmtLog.error(LOG, "Out of sync: latestVersion=%s, fileStore=%s", latestVersion, x);
            } else if ( x > VERSION_INIT ) {
                // Legal versions start 1.
                if ( latestVersion != x )
                    FmtLog.error(LOG, "Out of sync: latestVersion=%s, fileStore=%s", latestVersion, x);
            }
            else if ( latestVersion > VERSION_INIT ) {
                FmtLog.error(LOG, "Out of sync: latestVersion=%s, fileStore=%s", latestVersion, x);
            }
        }
    }
    
    @Override
    public PatchLogInfo getDescription() {
        return new PatchLogInfo(dsRef, name, getEarliestVersion(), getLatestVersion(), getLatestId());
    }

    @Override
    public boolean isEmpty() {
        return fileStore.isEmpty();
    }
    
    @Override
    public void release() {
        fileStore.release();
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
        try { 
            validate(patch.header(), patchId, previousId, this::badPatchEx);
            return true ;
        } catch (DeltaException ex) { return false; }
    }
    
    /**
     * Add a patch to the PatchLog.
     * This operation does not store the patch; 
     * it is expected to be already persisted.
     * Only the {@code PatchLog} in-memory metadata is updated.
     * @param patch
     */
    // XXX synchronized with fetching?
    // XXX Validation? "knownToBeValid" ?
    @Override
    synchronized
    public long append(RDFPatch patch) {
        // [DP-Fix]
        // If the patch is bad, we need to remove it
        // Timing hole.

        Id patchId = Id.fromNode(patch.getId());
        Id previousId = Id.fromNode(patch.getPrevious());

        FmtLog.info(LOG, "Append: id=%s prev=%s to log %s", patchId, previousId, getDescription());

        if ( ! Objects.equals(previousId, this.latestId) ) {
            String msg = String.format("Patch previous not log head: patch previous = %s ; log head = %s",
                                       previousId, this.latestId);
            // Does not point to right previous version.
            throw new DeltaBadPatchException(msg);
        }

        validateNewPatch(patchId, previousId, this::badPatchEx);

        // ** Commit point for a patch, 
        // specifically at the atomic "move file" in FileStore::writeNewFile.
        FileEntry entry = fileStore.writeNewFile(out -> {
            TokenWriter tw = new TokenWriterText(out) ;
            RDFChangesWriter dest = new RDFChangesWriter(tw) ;
            patch.apply(dest);
        });
        long version = entry.version;

        validateVersionNotInUse(version);

        idToVersion.put(patchId, version);
        headers.put(patchId, patch.header());
        latestId = patchId;
        latestVersion = version;
        validateLatest();
        return version; 
    }

    @FunctionalInterface
    interface BadHandler { void bad(String fmt, Object ...args) ; }
    
    private void validateNewPatch(Id patchId, Id previousId, BadHandler action) {
        if ( patchId == null )
            badPatchEx("No id");
        if ( idToVersion.containsKey(patchId) )
            action.bad("Patch already exists: patch=%s", patchId);
        if ( headers.containsKey(patchId) ) 
            action.bad("Patch header already exists: patch=%s", patchId);
    }
    
    private void validate(Id patchId, Id previousId, BadHandler action) {
        if ( patchId == null )
            badPatchEx("No id");

        if ( ! idToVersion.containsKey(patchId) )
            action.bad("Patch not found: patch=%s", patchId);
        if ( ! headers.containsKey(patchId) ) 
            action.bad("Patch header not found: patch=%s", patchId);
        
        PatchHeader header = headers.get(patchId);
        validate(header, patchId, previousId, action);
    }
        
    private void validate(PatchHeader header, Id patchId, Id previousId, BadHandler action) {
        // Non header
        if ( previousId != null ) {
            if ( ! idToVersion.containsKey(previousId) )
                action.bad("Patch previous not found: patch=%s, previous=%s", patchId, previousId);
            if ( ! headers.containsKey(previousId) ) 
                action.bad("Patch previous header not found: patch=%s, previous=%s", patchId, previousId);
            
            Node prevId = header.getPrevious() ;
            if ( previousId.asNode().equals(prevId) )
                action.bad("Patch previous header not found: patch=%s, previous=%s", patchId, previousId);
        } else {
            if ( header.getPrevious() != null )
                action.bad("Patch previous header not found: patch=%s, previous=%s", patchId, previousId);
        }
           
        if ( ! previousId.equals(getLatestId()) ) {
            // No empty log, previousId != null but does not match log head.
            // Validation should have caught this. 
            badPatchEx("Patch not an update on the latest logged one: id=%s prev=%s (log=[%d, %s])", 
                        patchId, previousId, getLatestVersion(), getLatestId());
        }
    }
    
    private void badPatchEx(String fmt, Object...args) {
        badPatchError(fmt, args);
        String msg = String.format(fmt, args);
        throw new DeltaBadPatchException(msg);
    }
    
    private void badPatchError(String fmt, Object...args) {
        FmtLog.error(LOG, String.format(fmt, args)); 
    }

    private void validateVersionNotInUse(long version) {
        if ( idToVersion.inverse().containsKey(version) )
            // Internal consistency error. FleStore was supposed to make it unique.
            throw new InternalErrorException("Version already in-use: "+version);
    }
    
    @Override
    public Stream<RDFPatch> range(Id start, Id finish) {
        Long startVersion = idToVersion.get(start);
        Long finishVersion = idToVersion.get(finish);
        if ( startVersion == null ) {}
        if ( finishVersion == null ) {}
        // Isolation not necessary. Patch files are immutable once written. 
        return
            LongStream
                .rangeClosed(startVersion, finishVersion)
                .mapToObj(v->fetch(fileStore, v));
    }
    
    private boolean validVersion(long version) {
        return version != VERSION_INIT && version != VERSION_UNSET && version <= latestVersion;  
    }
    
    @Override
    public Stream<RDFPatch> range(long start, long finish) {
        if ( ! validVersion(start) )
            throw new DeltaException("Bad start version : "+start);
        if ( ! validVersion(finish) )
            throw new DeltaException("Bad finish version : "+finish);
        // Isolation not necessary. Patch files are immutable once written. 
        return range$(start, finish);
    }

    private Stream<RDFPatch> range$(long startVersion, long finishVersion) {
        return
            LongStream
                .rangeClosed(startVersion, finishVersion)
                .mapToObj(v->fetch(fileStore, v));
    }
    
    public FileStore getFileStore() {
        return fileStore;
    }

    @Override
    public boolean contains(Id patchId) {
        return idToVersion.containsKey(patchId) ;
    }

    @Override
    public RDFPatch fetch(Id patchId) {
        Long version = idToVersion.get(patchId);
        if ( version == null )
            return null;
        return fetch(version) ;
    }

    @Override
    public RDFPatch fetch(long version) {
        return fetch(fileStore, version); 
    }
    
    private static RDFPatch fetch(FileStore fileStore, long version) {
        try ( InputStream in = fileStore.open((int)version) ) {
            RDFPatch patch = RDFPatchOps.read(in) ;
            return patch;
        }
        catch ( DeltaNotFoundException ex)  // Our internal 404.
        { return null; }
        catch (IOException ex) { 
            throw IOX.exception(ex); 
        }
    }

    @Override
    public long find(Id id) {
        Long x = idToVersion.get(id);
        if ( x == null )
            return VERSION_UNSET;
        return x.longValue() ;
    }
    
    @Override
    public Id find(long version) {
        return idToVersion.inverse().get(version);
    }
    
    @Override
    public String toString() {
        return String.format("PatchLog [%s, ver=%d head=%s]", name, getLatestVersion(), getLatestId());
    }

    public Id getDsRef() {
        return dsRef ;
    }
}
