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

import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.lib.NotImplemented;
import org.apache.jena.atlas.logging.FmtLog ;
import org.seaborne.delta.conn.DeltaConnection ;
import org.seaborne.delta.conn.Id ;
import org.seaborne.delta.conn.RegToken;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.RDFPatchOps ;
import org.seaborne.patch.changes.RDFChangesCollector ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

/** Implementation of {@link DeltaConnection}. */  
public class DeltaConnectionLocal implements DeltaConnection {
    private static Logger LOG = LoggerFactory.getLogger(DeltaConnectionLocal.class) ;
    //private static Logger LOG = DPS.LOG;
    
    public DeltaConnectionLocal() {}
    
    @Override
    public RDFChanges createRDFChanges(Id dsRef) {
        // XXX Stream!
        RDFChanges c = new RDFChangesCollector() {
            @Override
            public void finish() {
                RDFPatch p = getRDFPatch();
                sendPatch(dsRef, p);
            }
        };
        return c ;
    }

    @Override
    public RegToken register(Id clientId) {
        return DeltaConnectionMgr.get().register(clientId);
    }

    @Override
    public RegToken register(String name) {
        throw new NotImplemented();
    }

    @Override
    public void deregister(RegToken token) {
        DeltaConnectionMgr.get().deregister(token);
    }
    @Override
    public void deregister(Id clientId) {
        DeltaConnectionMgr.get().deregister(clientId);
    }

    @Override
    public void sendPatch(Id dsRef, RDFPatch rdfPatch) {
        DataSource source = DataRegistry.get().get(dsRef) ;
        if ( source == null )
            throw new DeltaExceptionBadRequest(404, "No such data source: "+dsRef) ;
        FmtLog.info(LOG, "receive: Dest=%s", source) ;
        source.getReceiver().receive(rdfPatch, null);
        // id -> registation
        System.out.println("Patch: "+rdfPatch.getId()) ;
        RDFPatchOps.write(System.out, rdfPatch) ;
        Patch patch = new Patch(rdfPatch, source, null);
        PatchSet ps = source.getPatchSet() ;
        ps.add(patch);
    }
    
    /** Process an {@code InputStream} and return an RDFPatch */
    private static Patch streamToPatch(DataSource source, InputStream in) {
        // Not RDFPatchOps.read(in) because receiver adds preprocessing.
        RDFChangesCollector collector = new RDFChangesCollector();
        Receiver receiver = source.getReceiver();
        FileEntry entry = receiver.receive(in, collector);
        return new Patch(collector.getRDFPatch(), source, entry);
    }
    
    @Override
    public int getCurrentVersion(Id dsRef) {
        DataSource source = DataRegistry.get().get(dsRef) ;
        if ( source == null )
            throw new DeltaExceptionBadRequest(404, "No such data source: "+dsRef) ;
        return getCurrentVersion(source);
    }

    private int getCurrentVersion(DataSource source) {
        return source.getPatchSet().getFileStore().getCurrentIndex();
    }
    
    /** Retrieve a patch and write it to the {@code OutptuSteram}. */ 
    @Override
    public RDFPatch fetch(Id dsRef, Id patchId) {
        DataSource source = DataRegistry.get().get(dsRef) ;
        if ( source == null )
            throw new DeltaExceptionBadRequest(404, "No such data source: "+dsRef) ;
        Patch patch = source.getPatchSet().fetch(patchId) ;
        if ( patch == null )
            throw new DeltaExceptionBadRequest(404, "No such patch: "+patchId) ;
        FmtLog.info(LOG, "fetch: Dest=%s, Patch=%s", source, patchId) ;
        return patch ;
    }

    /** Retrieve a patch and write it to the {@code OutptuSteram}. */ 
    @Override
    public RDFPatch fetch(Id dsRef, int version) {
        DataSource source = DataRegistry.get().get(dsRef) ;
        if ( source == null )
            throw new DeltaExceptionBadRequest(404, "No such data source: "+dsRef) ;
        RDFPatch patch = source.getPatchSet().fetch(version);
        FmtLog.info(LOG, "fetch: Dest=%s, Version=%d, Patch=%s", source, version, patch.getId()) ;
        return patch;
    }

    @Override
    public JsonArray getDatasets() {
        return null;
    }
}
