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

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.system.Txn;
import org.seaborne.delta.DPNames;
import org.seaborne.delta.Delta;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.DeltaLinkHTTP ;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.Id;
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
    
    // Local base dataset
    private static DatasetGraph dsg0 = null;
    // Local patch dataset
    private static DatasetGraph dsg1 = null;
    // Local tracking dataset
    private static DatasetGraph dsg2 = null;
    
    // Registration.
    // Direct to local for testing.
    //   Better - pass in a DeltaConnection
    
    public static void mainMain() {
        server();
        Delta.DELTA_LOG.info("==== Client ====");
        
        Id clientId = Id.fromUUID(C.uuid_c1); 
        Id dsRef = Id.fromUUID(C.uuid1);
        clientDataset(clientId, dsRef);
        
        Delta.DELTA_LOG.info("==== Bootstrap ====");
        dsg2 = bootfrom(clientId, dsRef);
        System.out.println("Local");
        print(dsg0);
        System.out.println("Tracked");
        print(dsg2);
    }
    
    private static void clientDataset(Id clientId, Id dsRef) {
        // Register.
        dsg0 = DatasetGraphFactory.createTxnMem();
        
        // Match with a dataset
        DeltaLink dc = new DeltaLinkHTTP("http://localhost:4040/");
        DeltaConnection client = DeltaConnection.create("RDFP", clientId, dsRef, dsg0, dc);
        
        dsg1 = client.getDatasetGraph();
        Quad quad1 = SSE.parseQuad("(_ :ss :pp 11)");
        Quad quad2 = SSE.parseQuad("(_ :ss :pp 22)");
        
        // Need to set an id.
        Txn.executeWrite(dsg1,  ()-> {
            dsg1.getDefaultGraph().getPrefixMapping().setNsPrefix("ex", "http://example/");
            dsg1.add(quad1);
        });
        Txn.executeWrite(dsg1,  ()->dsg1.add(quad2));
    }

    private static void server() {
    
        // Setup.
        System.setProperty(DPNames.ENV_PORT, "4040");
        System.setProperty(DPNames.ENV_HOME, "/home/afs/ASF/rdf-delta/");

        String BASE = "/home/afs/ASF/rdf-delta/DeltaServer";
        System.setProperty(DPNames.ENV_BASE, BASE);
        String config = "/home/afs/ASF/rdf-delta/delta.cfg";
        if ( ! FileOps.exists(config) ) {
            System.err.println("No configuration file: "+config);
            System.exit(2);
        }
        System.setProperty(DPNames.ENV_CONFIG, config);
        
        FileOps.ensureDir(BASE);
        Path p = Paths.get(BASE);
        Path pSources = p.resolve(DPNames.SOURCES).toAbsolutePath();
        //Path pPatches = p.resolve(DPNames.PATCHES).toAbsolutePath();
        FileOps.ensureDir(pSources.toString());
        //FileOps.ensureDir(pPatches.toString());
        //FileOps.clearDirectory(pPatches.toString());

        // Server.
        org.seaborne.delta.server.http.CmdDeltaServer.main("--base="+BASE, "--port=4040", "--config="+config);
    }
    
    private static void print(DatasetGraph dsg) {
        System.out.println("-- -- -- -- -- -- -- -- -- -- --");
        RDFDataMgr.write(System.out, dsg, Lang.TRIG);
        System.out.println("-- -- -- -- -- -- -- -- -- -- --");
    }
//        
//        if ( false ) {
//            // Send patch. No registration.
//            Id dsRef = Id.fromUUID(C.uuid1); 
//            RDFPatch patch = RDFPatchOps.fileToPatch("data.rdfp");
//            RDFChanges remote = new RDFChangesHTTP("http://localhost:4040/patch?dataset="+dsRef.asParam()) ;
//            patch.apply(remote);
//        }
//        
//        
//        Id clientId = Id.fromUUID(C.uuid_c1); 
//        DeltaClient client = DeltaClient.create("RDFP", clientId, dsRef, dsg0, dc);
//        DatasetGraph dsg = client.getDatasetGraph();
//        
//        Quad quad1 = SSE.parseQuad("(_ :ss :pp 11)");
//        Quad quad2 = SSE.parseQuad("(_ :ss :pp 22)");
//        
//        // Need to set an id.
//        Txn.executeWrite(dsg,  ()->dsg.add(quad1));
//        Txn.executeWrite(dsg,  ()->dsg.add(quad2));
//
//        if ( false ) {
//            // Directly, not via DeltaClient.
//            // To get the id.
//            RDFPatch patch = RDFPatchOps.fileToPatch("data.rdfp");
//            //Id patchId = Id.fromNode(patch.getId()); 
//            client.sendPatch(patch);
//        }
//        
//        // Poll **** ?zone=&dataset= -> version
//        // Fetch **** ?zone=&dataset=&version=
//        // Client API == API.java
//        
////        int ver = client.getRemoteVersionLatest();
//        
//        int ver = client.getRemoteVersionLatest();
//        System.out.println("ver="+ver);
//        
//        System.out.println();
//        System.out.println("-- -- -- -- -- -- -- -- -- -- --");
//        RDFDataMgr.write(System.out, dsg, Lang.TRIG);
//        System.out.println("-- -- -- -- -- -- -- -- -- -- --");
//        
//        // ---
//        DatasetGraph dsg2 = bootfrom(clientId, dsRef) ;
//        System.out.println("-- -- -- -- -- -- -- -- -- -- --");
//        RDFDataMgr.write(System.out, dsg2, Lang.TRIG);
//        System.out.println("-- -- -- -- -- -- -- -- -- -- --");
//    }
//
    private static DatasetGraph bootfrom(Id clientId, Id dsRef) {
        DatasetGraph dsg0 = DatasetGraphFactory.createTxnMem();
        dsg0.getDefaultGraph().getPrefixMapping().setNsPrefix("ex", "http://example/");
        DeltaLink dc = new DeltaLinkHTTP("http://localhost:4040/");
        DeltaConnection client = DeltaConnection.create("RDFP", clientId, dsRef, dsg0, dc);
        DatasetGraph dsg = client.getDatasetGraph();
        return dsg;
    }
}
