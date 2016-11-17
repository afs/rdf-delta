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

package dev;

import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb.base.file.Location ;
import org.seaborne.delta.Delta;
import org.seaborne.delta.client.DeltaClient;
import org.seaborne.delta.client.DeltaConnectionHTTP ;
import org.seaborne.delta.client.RDFChangesHTTP;
import org.seaborne.delta.conn.DeltaConnection ;
import org.seaborne.delta.conn.Id ;
import org.seaborne.delta.server.C ;
import org.seaborne.delta.server.DataRegistry ;
import org.seaborne.delta.server.DataSource ;
import org.seaborne.delta.server.DeltaConnectionLocal ;
import org.seaborne.delta.server.http.DataPatchServer ;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunServer2 {
    static { DevLib.setLogging(); }
    private static Logger LOG = LoggerFactory.getLogger("Main") ; 
    
    public static void main(String ...args) {
        try { mainMain(); }
        catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
        System.out.println("DONE");
        System.exit(0) ;
    }
     
    public static void mainMain() {
        // Zone -> DataRegistry 
        // DataRegistry -> DataSource
        // DataSource = one changing 
        
        String SOURCES = "/home/afs/ASF/rdf-delta/Sources" ;
        String PATCHES = "/home/afs/ASF/rdf-delta/Sources/Patches" ;
        FileOps.ensureDir(PATCHES);
        
        FileOps.clearDirectory(PATCHES);
        
        // Setup - need better registration based on scan-find.
        Location sourceArea = Location.create(SOURCES) ;
        DataSource source = DataSource.attach(sourceArea) ;
        //System.out.println(source) ;

        DataRegistry dReg = DataRegistry.get();
        dReg.put(source.getId(), source);

        // Server.
        DeltaConnection impl =  new DeltaConnectionLocal();
        DataPatchServer dps = new DataPatchServer(4040, impl) ;
        dps.start(); 
        Delta.DELTA_LOG.info("==== ====");

        //String ds = C.uuid1.toString();
        Id dsRef = Id.fromUUID(C.uuid1); 
        
        if ( false ) {
            // Send patch.
            RDFPatch patch = RDFPatchOps.fileToPatch("data.rdfp");
            RDFChanges remote = new RDFChangesHTTP("http://localhost:4040/patch?dataset="+dsRef.asParam()) ;
            patch.apply(remote);
        }
        
        DatasetGraph dsg0 = DatasetGraphFactory.createTxnMem();
        dsg0.getDefaultGraph().getPrefixMapping().setNsPrefix("ex", "http://example/");
        DeltaConnection dc = new DeltaConnectionHTTP("http://localhost:4040/");
        
        Id clientId = Id.fromUUID(C.uuid_c1); 
        DeltaClient client = DeltaClient.create("RDFP", clientId, dsRef, dsg0, dc);
        DatasetGraph dsg = client.getDatasetGraph();
        
        Quad quad1 = SSE.parseQuad("(_ :ss :pp 11)");
        Quad quad2 = SSE.parseQuad("(_ :ss :pp 22)");
        
        // Need to set an id.
        Txn.executeWrite(dsg,  ()->dsg.add(quad1));
        Txn.executeWrite(dsg,  ()->dsg.add(quad2));

        if ( false ) {
            // Directly, not via DeltaClient.
            // To get the id.
            RDFPatch patch = RDFPatchOps.fileToPatch("data.rdfp");
            //Id patchId = Id.fromNode(patch.getId()); 
            client.sendPatch(patch);
        }
        
        // Poll **** ?zone=&dataset= -> version
        // Fetch **** ?zone=&dataset=&version=
        // Client API == API.java
        
//        int ver = client.getRemoteVersionLatest();
        
        int ver = client.getRemoteVersionLatest();
        System.out.println("ver="+ver);
        
        System.out.println();
        System.out.println("-- -- -- -- -- -- -- -- -- -- --");
        RDFDataMgr.write(System.out, dsg, Lang.TRIG);
        System.out.println("-- -- -- -- -- -- -- -- -- -- --");
        
        // ---
        DatasetGraph dsg2 = bootfrom(clientId, dsRef) ;
        System.out.println("-- -- -- -- -- -- -- -- -- -- --");
        RDFDataMgr.write(System.out, dsg, Lang.TRIG);
        System.out.println("-- -- -- -- -- -- -- -- -- -- --");
    }

    private static DatasetGraph bootfrom(Id clientId, Id dsRef) {
        DatasetGraph dsg0 = DatasetGraphFactory.createTxnMem();
        dsg0.getDefaultGraph().getPrefixMapping().setNsPrefix("ex", "http://example/");
        DeltaConnection dc = new DeltaConnectionHTTP("http://localhost:4040/");
        DeltaClient client = DeltaClient.create("RDFP", clientId, dsRef, dsg0, dc);
        DatasetGraph dsg = client.getDatasetGraph();
        return dsg;
    }
}
