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

package org.seaborne.delta.server.local.patchstores.file;

import static org.seaborne.delta.DeltaConst.VERSION_INIT ;

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
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.*;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.server.local.DataSource ;
import org.seaborne.delta.server.local.PatchLog;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchValidation;
import org.seaborne.delta.server.local.filestore.FileEntry;
import org.seaborne.delta.server.local.filestore.FileStore;
import org.seaborne.patch.PatchHeader;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesWriter ;
import org.seaborne.patch.text.RDFPatchReaderText ;
import org.seaborne.patch.text.TokenWriter ;
import org.seaborne.patch.text.TokenWriterText ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A sequence of patches for an {@link DataSource}. */
public class PatchLogFile implements PatchLog {
    // Predates PatchStorage/PatchLogIndex.
    // Could do with converting.

    private static final boolean CHECKING = true ;

    // Centralized logger for regular lifecycle reporting.
    private static Logger  LOG     = LoggerFactory.getLogger(PatchLogFile.class);

    // Currently, id of the DataSource
    private final Id logId;
    private final DataSourceDescription dsd;
    private final PatchStore      patchStore;
    private final FileStore       fileStore;

    // Forward, backwards chain?
    // c.g. HistoryEntry
    private BiMap<Id, Version> idToVersion =  Maps.synchronizedBiMap(HashBiMap.create());
    private Map<Id, PatchHeader> headers = new ConcurrentHashMap<>();

    private Id latestId = null;
    private Version latestVersion = Version.UNSET;

    /** Attached to an existing {@code PatchLog}. */
    public static PatchLogFile attach(DataSourceDescription dsd, PatchStore patchStore, Location location) {
        return new PatchLogFile(dsd, patchStore, location);
    }

    private PatchLogFile(DataSourceDescription dsd, PatchStore patchStore, Location location) {
        this.dsd = dsd;
        this.logId = dsd.getId();
        this.fileStore = FileStore.attach(location, "patch");
        this.patchStore = patchStore;
        initFromFileStore();
    }

    @Override
    public Id getLogId() {
        return logId;
    }

    private void initFromFileStore() {
        Iterator<Long> iter = fileStore.getIndexes().iterator();
        PatchHeader previous = null;
        for ( ; iter.hasNext() ; ) {
            long idx = iter.next();
            try ( InputStream in = fileStore.open(idx) ) {
                PatchHeader patchHeader = RDFPatchReaderText.readerHeader(in);
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
                Version ver = Version.create(idx);
                idToVersion.put(id, ver);
                latestId = id;
                latestVersion = ver;
            }
            catch (NoSuchFileException ex) {}
            catch (IOException ex) {}
        }
    }

    @Override
    public Id getEarliestId() {
        Version x = getEarliestVersion();
        if ( ! Version.isValid(x) )
            return null;
        return idToVersion.inverse().get(x);
    }

    @Override
    public Version getEarliestVersion() {
        return Version.create(fileStore.getMinIndex());
    }

    @Override
    public Id getLatestId() {
        validateLatest();
        return latestId;
    }

    @Override
    public Version getLatestVersion() {
        validateLatest();
       return Version.create(fileStore.getCurrentIndex());
    }

    private void validateLatest() {
        if ( CHECKING ) {
            synchronized(this) {
                long x = fileStore.getCurrentIndex();
                if ( x == latestVersion.value() )
                    return ;
                // latestVersion = -1 (UNSET) and getCurrentIndex==0 (INIT) is OK
                if ( latestVersion == Version.UNSET && x == VERSION_INIT )
                    return ;
                FmtLog.error(LOG, "Out of sync: latestVersion=%s, fileStore=%s", latestVersion, x);
            }
        }
    }

    @Override
    public PatchLogInfo getInfo() {
        // AbstractPatchLog
        return new PatchLogInfo(dsd, getEarliestVersion(), getLatestVersion(), getLatestId());
    }

    @Override
    public DataSourceDescription getDescription() {
        return dsd;
    }

    @Override
    public boolean isEmpty() {
        return fileStore.isEmpty();
    }

    @Override
    public void delete() {
        CfgFile.retire(fileStore.getPath());
        release();
    }

    @Override
    public void release() {
        fileStore.release();
    }

    /** Validate a patch for this {@code PatchLog} */
    private boolean validate(RDFPatch patch) {
        return PatchValidation.validate(patch, this);
    }

    /**
     * Add a patch to the PatchLog.
     * This operation does not store the patch;
     * it is expected to be already persisted.
     * Only the {@code PatchLog} in-memory metadata is updated.
     * @param patch
     */
    // XXX synchronized with fetching?
    @Override
    synchronized
    public Version append(RDFPatch patch) {
        // XXX If the patch is bad, we need to remove it. Timing hole?

        Id patchId = Id.fromNode(patch.getId());
        Id previousId = Id.fromNode(patch.getPrevious());

        // Is it a reply of the last patch?
        if ( ! isEmpty() && getLatestId().equals(patchId) ) {
            return getLatestVersion();
        }

        if ( LOG.isDebugEnabled() )
            FmtLog.debug(LOG, "append: id=%s prev=%s to log %s", patchId, previousId, getInfo());

        PatchValidation.validateNewPatch(this, patchId, previousId, PatchValidation::badPatchEx);

        if ( ! Objects.equals(previousId, this.latestId) ) {
            String msg = String.format("Patch previous not log head: patch previous = %s ; log head = %s",
                                       previousId, this.latestId);
            // Does not point to right previous version.
            throw new DeltaBadRequestException(msg);
        }

        // ** Commit point for a patch,
        // specifically at the atomic "move file" in FileStore::writeNewFile.
        FileEntry entry = fileStore.writeNewFile(out -> {
            TokenWriter tw = new TokenWriterText(out) ;
            RDFChangesWriter dest = new RDFChangesWriter(tw) ;
            patch.apply(dest);
        });

        Version version = Version.create(entry.version);
        validateVersionNotInUse(version);
        idToVersion.put(patchId, version);
        headers.put(patchId, patch.header());
        latestId = patchId;
        latestVersion = version;
        validateLatest();
        return version;
    }

    private void validateVersionNotInUse(Version version) {
        if ( idToVersion.inverse().containsKey(version) )
            // Internal consistency error. FleStore was supposed to make it unique.
            throw new InternalErrorException("Version already in-use: "+version);
    }

    @Override
    public Stream<RDFPatch> range(Id start, Id finish) {
        Version startVersion = idToVersion.get(start);
        Version finishVersion = idToVersion.get(finish);
        if ( startVersion == null ) {}
        if ( finishVersion == null ) {}
        // Isolation not necessary. Patch files are immutable once written.
        return
            LongStream
                .rangeClosed(startVersion.value(), finishVersion.value())
                .mapToObj(v->fetch(fileStore, v));
    }

    @Override
    public Stream<RDFPatch> range(Version start, Version finish) {
        if ( ! Version.isValid(start) )
            throw new DeltaException("Bad start version : "+start);
        if ( ! Version.isValid(finish) )
            throw new DeltaException("Bad finish version : "+finish);
        // Isolation not necessary. Patch files are immutable once written.
        return range$(start, finish);
    }

    private Stream<RDFPatch> range$(Version startVersion, Version finishVersion) {
        return
            LongStream
                .rangeClosed(startVersion.value(), finishVersion.value())
                .mapToObj(v->fetch(fileStore, v));
    }

    public FileStore getFileStore() {
        return fileStore;
    }

    @Override
    public PatchStore getPatchStore() {
        return patchStore;
    }

    @Override
    public boolean contains(Id patchId) {
        return idToVersion.containsKey(patchId) ;
    }

    @Override
    public RDFPatch fetch(Id patchId) {
        Version version = idToVersion.get(patchId);
        if ( version == null )
            return null;
        return fetch(version) ;
    }

    @Override
    public RDFPatch fetch(Version version) {
        return fetch(fileStore, version.value());
    }

    private RDFPatch fetch(FileStore fileStore, long version) {
        if ( version < getEarliestVersion().value() )
            return null;
        if ( version > getLatestVersion().value() )
            return null;

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
    public Version find(Id id) {
        Version x = idToVersion.get(id);
        if ( x == null )
            return Version.UNSET;
        return x;
    }

    @Override
    public Id find(Version version) {
        return idToVersion.inverse().get(version);
    }

    @Override
    public String toString() {
        return String.format("PatchLog [%s, ver=%d head=%s]", dsd.getName(), getLatestVersion(), getLatestId());
    }

    public Id getDsRef() {
        return dsd.getId() ;
    }
}
