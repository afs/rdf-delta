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

import static org.apache.jena.atlas.lib.ListUtils.toList;
import static org.seaborne.delta.Id.str;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.*;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.DeltaLinkListener;
import org.seaborne.delta.link.DeltaNotConnectedException;
import org.apache.jena.rdfpatch.RDFPatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of {@link DeltaLink} backed by a {@link LocalServer}. */
public class DeltaLinkLocal implements DeltaLink {
    private static final int  BUF_SIZE = 128 * 1024;

    private static Logger     LOG      = LoggerFactory.getLogger(DeltaLinkLocal.class);

    // Switch all the levels.
    private static void devlog(Logger log, String fmt, Object...args) {
        if ( ! log.isDebugEnabled() )
            return;
        FmtLog.debug(log, fmt, args);
    }

    private final LocalServer localServer;
    private volatile boolean  linkOpen = false;

    private Set<DeltaLinkListener> listeners = ConcurrentHashMap.newKeySet();

    public static DeltaLink connect(LocalServer localServer) {
        return new DeltaLinkLocal(localServer);
    }

    private DeltaLinkLocal(LocalServer localServer) {
        this.localServer = localServer;
        this.linkOpen = true;
    }

    @Override
    public void start() {
        localServer.start();
        localServer.logDetails();
        linkOpen = true;
    }

    @Override
    public void close() {
        linkOpen = false;
        localServer.shutdown();
    }

    @Override
    public JsonObject ping() {
        return DeltaLib.ping();
    }

    public LocalServer getLocalServer() {
        return localServer;
    }

    private void checkLink() {
        if ( !linkOpen )
            throw new DeltaNotConnectedException("Not connected");
    }

    @Override
    public Id newDataSource(String name, String baseURI) {
        checkLink();
        if ( !DeltaOps.isValidName(name) )
            throw new IllegalArgumentException("Invalid data source name: '" + name + "'");

        Id dsRef = localServer.createDataSource(name, baseURI);
        event(listener->listener.newDataSource(dsRef, name));
        return dsRef;
    }

    @Override
    public Id copyDataSource(Id dsRef, String srcName, String dstName) {
        checkLink();
        Objects.requireNonNull(dsRef, "dsRef");
        Objects.requireNonNull(srcName, "srcName");
        Objects.requireNonNull(dstName, "dstName");
        if ( !DeltaOps.isValidName(srcName) )
            throw new IllegalArgumentException("Invalid data source name : '" + srcName + "'");
        if ( !DeltaOps.isValidName(dstName) )
            throw new IllegalArgumentException("Invalid data source name : '" + dstName + "'");
        DataSource dSrc = localServer.getDataSource(dsRef);
        Id dsRef2 = localServer.copyDataSource(dsRef, srcName, dstName);
        event(listener->listener.copyDataSource(dsRef, dsRef2, srcName, dstName));
        return dsRef2;
    }

    @Override
    public Id renameDataSource(Id dsRef, String oldName, String newName) {
        checkLink();
        Objects.requireNonNull(dsRef, "dsRef");
        Objects.requireNonNull(oldName, "oldName");
        Objects.requireNonNull(newName, "newName");
        if ( !DeltaOps.isValidName(oldName) )
            throw new IllegalArgumentException("Invalid data source name : '" + oldName + "'");
        if ( !DeltaOps.isValidName(newName) )
            throw new IllegalArgumentException("Invalid data source name : '" + newName + "'");
        DataSource dSrc = localServer.getDataSource(dsRef);

        Id dsRef2 = localServer.renameDataSource(dsRef, oldName, newName);
        event(listener->listener.renameDataSource(dsRef, dsRef2, oldName, newName));
        return dsRef2;
    }

    @Override
    public void removeDataSource(Id dsRef) {
        checkLink();
        localServer.removeDataSource(dsRef);
        event(listener->listener.removeDataSource(dsRef));
    }

    @Override
    public List<Id> listDatasets() {
        checkLink();
        return localServer.listDataSourcesIds();
    }

    @Override
    public List<DataSourceDescription> listDescriptions() {
        checkLink();
        return toList(localServer.listDataSources().stream().map(ds -> ds.getDescription()));
    }

    @Override
    public List<PatchLogInfo> listPatchLogInfo() {
        checkLink();
        return localServer.listPatchLogInfo();
    }

    @Override
    public DataSourceDescription getDataSourceDescription(Id dsRef) {
        checkLink();
        DataSource source = localServer.getDataSource(dsRef);
        return description(source);
    }

    @Override
    public DataSourceDescription getDataSourceDescriptionByURI(String uri) {
        checkLink();
        DataSource source = localServer.getDataSourceByURI(uri);
        return description(source);
    }

    @Override
    public DataSourceDescription getDataSourceDescriptionByName(String name) {
        checkLink();
        DataSource source = localServer.getDataSourceByName(name);
        return description(source);
    }

    private static DataSourceDescription description(DataSource source) {
        if ( source == null )
            return null;
        return source.getDescription();
    }

    @Override
    public PatchLogInfo getPatchLogInfo(Id dsRef) {
        checkLink();
        DataSource source = getDataSource(dsRef);
        if ( source == null )
            return null;
        return source.getPatchLog().getInfo();
    }

    @Override
    public Version append(Id dsRef, RDFPatch rdfPatch) {
        checkLink();
        DataSource source = getDataSource(dsRef);
        // Patch not known to be valid yet.
        // Patch not safe in the Patch Log yet.
        PatchLog patchLog = source.getPatchLog();
        try {
            beforeWrite(source, patchLog, rdfPatch);

            long t1 = System.currentTimeMillis();
            Version version = patchLog.append(rdfPatch);
            long t2 = System.currentTimeMillis();

            afterWrite(source, rdfPatch, version, (t2 - t1));
            event(listener-> listener.append(dsRef, version, rdfPatch));
            return version;
        }
        catch (RuntimeException ex) {
            badWrite(source, patchLog, rdfPatch, ex);
            devlog(LOG, "append: Failed: Dest=%s Patch=%s ; %s", source, str(rdfPatch.getId()), ex.getMessage());
            // Stack trace logged higher up if relevant.
            throw ex;
        }
    }

    /**
     * Called before writing the patch to the {@link PatchLog}. There is no guarantee
     * that the patch is valid and will be committed to the PatchLog.
     */
    protected void beforeWrite(DataSource source, PatchLog patchLog, RDFPatch rdfPatch) {
        // devlog(LOG, "append: start: Patch=%s ds=%s", str(rdfPatch.getId()),
    }

    /** Called after writing the patch to the {@link PatchLog}. */
    protected void afterWrite(DataSource source, RDFPatch rdfPatch, Version version, long timeElapsed) {
        // log(LOG, "append: finish: Patch=%s[ver=%d] ds=%s",
        // str(rdfPatch.getId()), version, source);
        //log(LOG, "append (%.3fs): Patch=%s[%s] ds=%s", (timeElapsed / 1000.0), str(rdfPatch.getId()), version, source);
        devlog(LOG, "append : Patch=%s(>%s)[%s] ds=%s", str(rdfPatch.getId()), str(rdfPatch.getPrevious()), version, source);
    }

    /** Called after attempting to write the patch to the {@link PatchLog}. */
    protected void badWrite(DataSource source, PatchLog patchLog, RDFPatch rdfPatch, RuntimeException ex) {
        devlog(LOG, "Bad write: patch=%s ds=%s : msg=%s", str(rdfPatch.getId()), source, ex.getMessage());
    }

    private DataSource getDataSource(Id dsRef) {
        DataSource source = localServer.getDataSource(dsRef);
        if ( source == null )
            throw new DeltaNotFoundException("No such data source: " + dsRef);
        return source;
    }

    private DataSource getDataSourceOrNull(Id dsRef) {
        return localServer.getDataSource(dsRef);
    }

    /** Retrieve a patch by patchId. */
    @Override
    public RDFPatch fetch(Id dsRef, Id patchId) {
        checkLink();
        DataSource source = getDataSourceOrNull(dsRef);
        if ( source == null )
            return null;
        RDFPatch patch = source.getPatchLog().fetch(patchId);
        if ( patch == null )
            return null;
            //throw new DeltaNotFoundException("No such patch: " + patchId);
        devlog(LOG, "fetch: Dest=%s, Patch=%s", source, patchId);
        event(listener->listener.fetchById(dsRef, patchId, patch));
        return patch;
    }

    /** Retrieve a patch by version. */
    @Override
    public RDFPatch fetch(Id dsRef, Version version) {
        checkLink();
        DataSource source = getDataSourceOrNull(dsRef);
        if ( source == null )
            return null;
        RDFPatch patch = source.getPatchLog().fetch(version);
        if ( LOG.isInfoEnabled() ) {
            if ( patch == null ) {
                devlog(LOG, "fetch: Dest=%s, %s, Not found", source, version);
            } else {
                Id id = Id.fromNode(patch.getId());
                devlog(LOG, "fetch: Dest=%s, %s, Patch=%s", source, version, id);
            }
        }
        event(listener->listener.fetchByVersion(dsRef, version, patch));
        return patch;
    }

    private RDFPatch fetchCommon(Id dsRef, Id patchId, Version version) {
        checkLink();
        DataSource source = getDataSourceOrNull(dsRef);
        if ( source == null )
            return null;
        RDFPatch patch = source.getPatchLog().fetch(version);
        if ( LOG.isInfoEnabled() ) {
            if ( patch == null ) {
                devlog(LOG, "fetch: Dest=%s, %s, Not found", source, version);
            } else {
                Id id = Id.fromNode(patch.getId());
                devlog(LOG, "fetch: Dest=%s, %s, Patch=%s", source, version, id);
            }
        }
        event(listener->listener.fetchById(dsRef, patchId, patch));
        return patch;
    }

    @Override
    public String initialState(Id dsRef) {
        // Not implemented.
        return null;
    }

    private <X> void event(Consumer<DeltaLinkListener> action) {
        listeners.forEach(action);
    }

    @Override
    public void addListener(DeltaLinkListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(DeltaLinkListener listener) {
        listeners.remove(listener);
    }

    @Override
    public Id acquireLock(Id datasourceId) {
        Objects.requireNonNull(datasourceId);
        checkLink();
        DataSource source = getDataSource(datasourceId);
        if ( source == null )
            return null;
        Id session = source.getPatchLog().acquireLock();
        return session;
    }

    @Override
    public boolean refreshLock(Id datasourceId, Id session) {
        Objects.requireNonNull(datasourceId);
        Objects.requireNonNull(session);
        checkLink();
        DataSource source = getDataSourceOrNull(datasourceId);
        if ( source == null )
            return false;
        boolean status = source.getPatchLog().refreshLock(session);
        return status;
    }

//    @Override
//    public Set<Id> refreshLocks(Set<Id> lockSet) {
//        //Need Id -> datasource.
//        // TEMP
//        LOG.warn("refreshLocks - implementation required");
//        return Collections.emptySet();
//    }

    @Override
    public LockState readLock(Id datasourceId) {
        Objects.requireNonNull(datasourceId);
        checkLink();
        DataSource source = getDataSource(datasourceId);
        return source.getPatchLog().readLock();
    }

    @Override
    public Id grabLock(Id datasourceId, Id oldSession) {
        Objects.requireNonNull(datasourceId);
        Objects.requireNonNull(oldSession);
        checkLink();
        DataSource source = getDataSource(datasourceId);
        return source.getPatchLog().grabLock(oldSession);
    }

    @Override
    public void releaseLock(Id datasourceId, Id session) {
        Objects.requireNonNull(datasourceId);
        Objects.requireNonNull(session);
        checkLink();
        DataSource source = getDataSourceOrNull(datasourceId);
        if ( source == null )
            return;
        PatchLog log = source.getPatchLog();
        log.releaseLock(session);
    }
}
