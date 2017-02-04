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

import java.io.InputStream ;
import java.util.List;

import org.apache.jena.atlas.logging.FmtLog ;
import org.apache.jena.graph.Node;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.Id;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.DeltaLinkBase;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.RDFPatchOps ;
import org.seaborne.patch.changes.RDFChangesCollector ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

/** Implementation of {@link DeltaLink}. */  
public class DeltaLinkLocal extends DeltaLinkBase implements DeltaLink {
    private static Logger LOG = LoggerFactory.getLogger(DeltaLinkLocal.class) ;
    
    private final LocalServer localServer;
    
    public static DeltaLink connect(LocalServer localServer) {
        return new DeltaLinkLocal(localServer);
    }

    private DeltaLinkLocal(LocalServer localServer) {
        this.localServer = localServer;
    }
    
    @Override
    public RDFChanges createRDFChanges(Id dsRef) {
        RDFChanges c = new RDFChangesCollector() {
            private Node currentTransactionId = null;
            
            @Override
            public void txnBegin() {
                super.txnBegin();
                if ( currentTransactionId == null ) {
                    currentTransactionId = Id.create().asNode();
                    super.header(RDFPatch.ID, currentTransactionId);
                }
            }

            @Override
            public void txnCommit() {
                super.txnCommit();
                RDFPatch p = getRDFPatch();
                sendPatch(dsRef, p);
            }
        };
        return c ;
    }

    @Override
    public Id newDataSource(String name, String baseURI) {
        checkRegistered();
        return localServer.createDataSource(false, name, baseURI);
    }

    @Override
    public void removeDataset(Id dsRef) {
        checkRegistered();
        localServer.removeDataSource(dsRef);
    }

    @Override
    public List<Id> listDatasets() {
        return localServer.listDataSourcesIds();
    }

    @Override
    public DataSourceDescription getDataSourceDescription(Id dsRef) {
        DataSource source = localServer.getDataSource(dsRef);
        if ( source == null )
            return null;
        return source.getDescription();
    }

    @Override
    public int sendPatch(Id dsRef, RDFPatch rdfPatch) {
        checkRegistered();
        DataSource source = getDataSource(dsRef);
        FmtLog.info(LOG, "receive: Dest=%s", source) ;
        FileEntry entry = source.getReceiver().receive(rdfPatch, null);
        // id -> registation
        FmtLog.info(LOG, "Patch: %s", rdfPatch.getIdNode()) ;
        
        // Debug
        if ( false ) {
            RDFPatchOps.write(System.out, rdfPatch) ;
        }
        PatchLog patchLog = source.getPatchLog() ;
        patchLog.addMeta(rdfPatch);
        return entry.version; 
    }

    /** Process an {@code InputStream} and return an RDFPatch */
    private static RDFPatch streamToPatch(DataSource source, InputStream in) {
        // Not RDFPatchOps.read(in) because receiver adds preprocessing.
        RDFChangesCollector collector = new RDFChangesCollector();
        Receiver receiver = source.getReceiver();
        FileEntry entry = receiver.receive(in, collector);
        return collector.getRDFPatch();
    }
    
    @Override
    public int getCurrentVersion(Id dsRef) {
        DataSource source = getDataSource(dsRef);
        return getCurrentVersion(source);
    }

    private DataSource getDataSource(Id dsRef) {
        DataSource source = localServer.getDataSource(dsRef);
        if ( source == null )
            throw new DeltaBadRequestException(404, "No such data source: " + dsRef);
        return source;
    }

    private FileStore getFileStore(Id dsRef) {
        return getFileStore(getDataSource(dsRef));
    }
    
    private FileStore getFileStore(DataSource source) {
        return source.getPatchLog().getFileStore();
    }

    private static int getCurrentVersion(DataSource source) {
        return source.getPatchLog().getFileStore().getCurrentIndex();
    }

    /** Retrieve a patch and write it to the {@code OutptuSteram}. */ 
    @Override
    public RDFPatch fetch(Id dsRef, Id patchId) {
        DataSource source = getDataSource(dsRef);
        RDFPatch patch = source.getPatchLog().fetch(patchId) ;
        if ( patch == null )
            throw new DeltaBadRequestException(404, "No such patch: "+patchId) ;
        FmtLog.info(LOG, "fetch: Dest=%s, Patch=%s", source, patchId) ;
        return patch ;
    }

    /** Retrieve a patch and write it to the {@code OutptuSteram}. */ 
    @Override
    public RDFPatch fetch(Id dsRef, int version) {
        DataSource source = getDataSource(dsRef) ;
        RDFPatch patch = source.getPatchLog().fetch(version);
        if ( LOG.isInfoEnabled() ) {
            Id id = Id.fromNode(patch.getIdNode());
            FmtLog.info(LOG, "fetch: Dest=%s, Version=%d, Patch=%s", source, version, id) ;
        }
        return patch;
    }

    private void checkRegistered() {
        if ( ! isRegistered() )
            throw new DeltaBadRequestException("Not registered");
    }
}
