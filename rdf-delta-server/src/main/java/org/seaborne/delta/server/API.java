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

package org.seaborne.delta.server;

import java.io.InputStream ;
import java.io.OutputStream;

import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.pubsub.Distributor ;
import org.seaborne.delta.pubsub.Receiver ;
import org.seaborne.patch.RDFPatchOps ;
import org.seaborne.patch.changes.RDFChangesCollector ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class API {
    
    private static Logger LOG = LoggerFactory.getLogger(API.class) ;
    
    static Distributor distributor = new Distributor() ;
    
    static {
        // Setup
    }
    
    public static class RegToken {
        
    }
    
    public static class Registration {
        RegToken regToken ;
        // Graphs, datasets? Channels?
        // new stuff?? 
    }
    
    public static Registration register(Id clientId) {
        distributor.register(null, null) ;
        return null ;
    }
    
    public static Registration register(String name) {
        return null ;
    }

    public Registration register(String name, Id id) {
        return null ;
    }
    
    public static void deregister(RegToken token) {}

    public static void receive(Id dsRef, InputStream in) {
        DataSource source = DataRegistry.get().get(dsRef) ;
        if ( source == null )
            throw new DeltaExceptionBadRequest(404, "No such data source: "+dsRef) ;
        FmtLog.info(LOG, "receive: Dest=%s", source) ;
        // id -> registation
        Patch patch = streamToPatch(source, in) ;
        // Now safe: on disk or whatever. 
        System.out.println("Patch: "+patch.getId()) ;
        RDFPatchOps.write(System.out, patch.get()) ;
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
    
    public static void write(Id dsRef, Id patchId) {
        fetch(dsRef, patchId, System.out);
    }
    
    public static void fetch(Id dsRef, Id patchId, OutputStream out) {
        DataSource source = DataRegistry.get().get(dsRef) ;
        if ( source == null )
            throw new DeltaExceptionBadRequest(404, "No such data source: "+dsRef) ;
        Patch patch = source.getPatchSet().fetch(patchId) ;
        if ( patch == null )
            throw new DeltaExceptionBadRequest(404, "No such patch: "+patchId) ;
        FmtLog.info(LOG, "fetch: Dest=%s, Patch=%s", source, patchId) ;
        System.out.println("Patch:") ;
        RDFPatchOps.write(out, patch) ;
    }
    
    
//    public static InChannel getChannel(Id data) {
//        DataRef ref = getDataRef(data) ;
//        if ( ref == null )
//            return null ;
//        return ref.channel() ;
//    }
    
    // Dataset system
    
    public void existingDataset() {} 
    
    public Id newDataset() { return null ; }
    public void deleteDataset(Id uuid) { }

    // Graph-only system
    
    public Id newGraph(String uri) { return null ; }

    

    
    // New graph(base Id)
    
    
    
}
