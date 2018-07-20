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

import static org.apache.jena.atlas.lib.ListUtils.toList;
import static org.seaborne.delta.Id.str;

import java.util.List;

import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.*;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.DeltaNotConnectedException;
import org.seaborne.patch.RDFPatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of {@link DeltaLink} backed by a {@link LocalServer}. */
public class DeltaLinkLocal implements DeltaLink {
    private static final int  BUF_SIZE = 128 * 1024;

    private static Logger     LOG      = LoggerFactory.getLogger(DeltaLinkLocal.class);

    private final LocalServer localServer;
    private boolean           linkOpen = false;

    public static DeltaLink connect(LocalServer localServer) {
        return new DeltaLinkLocal(localServer);
    }

    private DeltaLinkLocal(LocalServer localServer) {
        this.localServer = localServer;
        this.linkOpen = true;
    }

    @Override
    public Id newDataSource(String name, String baseURI) {
        checkLink();
        if ( !DeltaOps.isValidName(name) )
            throw new IllegalArgumentException("Invalid data source name: '" + name + "'");
        return localServer.createDataSource(name, baseURI);
    }

    @Override
    public void close() {
        linkOpen = false;
        localServer.shutdown();
    }

    @Override
    public void ping() {}

    public LocalServer getLocalServer() {
        return localServer;
    }

    private void checkLink() {
        if ( !linkOpen )
            throw new DeltaNotConnectedException("Not connected");
    }

    @Override
    public void removeDataSource(Id dsRef) {
        checkLink();
        localServer.removeDataSource(dsRef);
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
            // FmtLog.info(LOG, "append: start: Patch=%s ds=%s", str(rdfPatch.getId()),
            // source);
            long t1 = System.currentTimeMillis();

            Version version = patchLog.append(rdfPatch);

            long t2 = System.currentTimeMillis();
            afterWrite(source, rdfPatch, version, (t2 - t1));
            return version;
        }
        catch (RuntimeException ex) {
            badWrite(source, patchLog, rdfPatch, ex);
            FmtLog.info(LOG, "append: Failed: Dest=%s Patch=%s ; %s", source, str(rdfPatch.getId()), ex.getMessage());
            // Stack trace logged higher up if relevant.
            throw ex;
        }
    }

    /**
     * Called before writing the patch to the {@link PatchLog}. There is no guaranttee
     * that the patch is valid and will be commited to the PatchLog.
     */
    protected void beforeWrite(DataSource source, PatchLog patchLog, RDFPatch rdfPatch) {
        // FmtLog.info(LOG, "append: start: Patch=%s ds=%s", str(rdfPatch.getId()),
        // patchLog.getLogId().toString());
    }

    /** Called after writing the patch to the {@link PatchLog}. */
    protected void afterWrite(DataSource source, RDFPatch rdfPatch, Version version, long timeElapsed) {
        // FmtLog.info(LOG, "append: finish: Patch=%s[ver=%d] ds=%s",
        // str(rdfPatch.getId()), version, source);
        //FmtLog.info(LOG, "append (%.3fs): Patch=%s[%s] ds=%s", (timeElapsed / 1000.0), str(rdfPatch.getId()), version, source);
        FmtLog.info(LOG, "append : Patch=%s(>%s)[%s] ds=%s", str(rdfPatch.getId()), str(rdfPatch.getPrevious()), version, source);
    }

    /** Called after writing the patch to the {@link PatchLog}. */
    protected void badWrite(DataSource source, PatchLog patchLog, RDFPatch rdfPatch, RuntimeException ex) {
        FmtLog.info(LOG, "Bad write: patch=%s ds=%s : msg=%s", str(rdfPatch.getId()), source, ex.getMessage());
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
            throw new DeltaNotFoundException("No such patch: " + patchId);
        FmtLog.info(LOG, "fetch: Dest=%s, Patch=%s", source, patchId);
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
                FmtLog.info(LOG, "fetch: Dest=%s, %s, Not found", source, version);
            } else {
                Id id = Id.fromNode(patch.getId());
                FmtLog.info(LOG, "fetch: Dest=%s, %s, Patch=%s", source, version, id);
            }
        }
        return patch;
    }

    @Override
    public String initialState(Id dsRef) {
        // Not implemented.
        return null;
    }
}
