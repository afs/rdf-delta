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

import java.io.IOException;
import java.net.BindException ;

import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory ;
import org.apache.jena.sparql.core.Quad ;
import org.apache.jena.sparql.sse.SSE ;
import org.apache.jena.system.Txn ;
import org.apache.jena.tdb.base.file.Location ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo ;
import org.seaborne.delta.client.DeltaClient ;
import org.seaborne.delta.client.DeltaConnection ;
import org.seaborne.delta.client.DeltaLinkHTTP ;
import org.seaborne.delta.client.Zone ;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.PatchLogServer ;
import org.seaborne.delta.server.local.DeltaLinkLocal ;
import org.seaborne.delta.server.local.LocalServer ;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.RDFPatchOps ;

public class Run {
    static { 
        //LogCtl.setLog4j(); 
        LogCtl.setJavaLogging();
    }
    
    // previous checking.
    // Remove (hide) DSG from DeltaConnection start-up : TDB or file only.
    // Connect by name.
    
    // ** Need patch rejection
    // ** Start at id:nil (or id:datasource?)
    
    // TransPBlob not PersistentState : memory versions.
    
    // PatchCache
    
    // DataSource - better naming? Feed, RDFFeed?  DataFeed?
    //  = (zone,) name, id, PatchLog, start point, + optionally data. 
    
    //PathX. 
    
    // Next?
    
    // Can DataSources be shared across zones? Not unless the name is the same.  Acceptable?
    // DataSource Descriptior and LocalServer.SourceDescriptor are the same.
    
    static int PORT = 1068;
    
    public static void main(String... args) throws IOException {
        //JenaSystem.DEBUG_INIT = true ;
        //DeltaSystem.DEBUG_INIT = true ;
        //DeltaSystem.init();
        try {
            //main$misc();
            main$dc();
        } catch (Throwable ex) {
            System.out.println();
            System.out.flush();
            ex.printStackTrace();
            System.exit(1); }
        finally { System.exit(0); }
    }

    public static void main$dc() throws IOException {
        DeltaLink dLink = deltaLink(true, false);
        dLink.ping();
        if ( true ) return ; 
        
        
        
        FileOps.clearAll("Zone");
        Zone zone = Zone.create("Zone");
        
        Quad quad1 = SSE.parseQuad("(:g :s :p 111)");
        Quad quad2 = SSE.parseQuad("(:g :s :p 222)");
        Quad quad3 = SSE.parseQuad("(:g :s :p 333)");
        
        boolean httpServer = false;
        
        DeltaClient dc = DeltaClient.create(zone, dLink);
        
        Id dsRef = dLink.newDataSource("ABC", "http://example/ABC");
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        
        //dc.attach(dsRef, dsg);
        //dc.connect(dsRef);
        
        // Create zone-managed dataset.
        
        //dc.attach(dsRef);
        
        try ( DeltaConnection dConn = dc.get(dsRef) ) {
            dConn.sync();
            dsRef = dConn.getDataSourceId();
            Txn.executeWrite(dConn.getDatasetGraph(), ()->dConn.getDatasetGraph().add(quad1) );
        }
        
        try ( DeltaConnection dConn = dc.get(dsRef) ) {
            PatchLogInfo info = dConn.getPatchLogInfo();
            Id patchId = info.getLatestPatch();
            System.out.println("** fetch **");
            RDFPatch patch = dLink.fetch(dsRef, patchId);
            RDFPatchOps.write(System.out, patch);
            System.out.println("** fetch **");  
        }
    }

        // Quick run-through of some operations as a durign delveop, pre-test quick check.  
    public static void main$misc() throws IOException {
        // Do a delete.
        
        // --- Reset state.
        FileOps.clearAll("Zone");
        Zone zone = Zone.create("Zone");
        
        boolean httpServer = true;
        DeltaLink dLink = deltaLink(true, true);
        
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        
        System.out.println();
        Quad quad1 = SSE.parseQuad("(:g :s :p 111)");
        Quad quad2 = SSE.parseQuad("(:g :s :p 222)");
        Quad quad3 = SSE.parseQuad("(:g :s :p 333)");
        
        // Split create into create-> id, no connection. Adds to pool.
        // Always pool.
        // DLink..connect then only DeltaConnection.connect
        
        // DeltaConnection.connect
        // DeltaConnection.attach = connect + new state. = connect + dConn.setupLocal(dsg) -> ??
        // Take out registration.
        
        
        String datasourceName = "ABC";
        String datasourceURI = "http://example/ABC";

        //Id datasourceId = dLink1.newDataSource(datasourceName, datasourceURI);
        //DataState dataState = zone.create(datasourceId, datasourceName, datasourceURI, StorageType.TDB);
        
        boolean exists = dLink.listDescriptions().stream().anyMatch(x->datasourceName.equals(x.getName()));

        DeltaClient dClient = DeltaClient.create(zone, dLink);
        
//        //try ( DeltaConnection dConn = DeltaConnection.create(zone, datasourceName, datasourceURI, dsg, dLink1) ) {
//        try ( DeltaConnection dConn = x_connectOrCreate(zone, datasourceName, datasourceURI, dsg, dLink, !exists) ) {
//            String x = dConn.getInitialStateURL();
//            //System.out.println("State URL = "+x);
//            if ( x != null )
//                Txn.executeWrite(dConn.getDatasetGraph(), ()->RDFDataMgr.read(dConn.getDatasetGraph(), x));
//        }
//        
//        System.out.println();
        
        Id dsRef = dLink.getDataSourceDescriptionByURI(datasourceURI).getId();
        System.out.println();
        
        System.out.println("Sync");
        try ( DeltaConnection dConn = dClient.get(dsRef) ) {
            dConn.sync();
            dsRef = dConn.getDataSourceId();
            Txn.executeWrite(dConn.getDatasetGraph(), ()->dConn.getDatasetGraph().add(quad1) );
        }
        
        
        try ( DeltaConnection dConn = dClient.get(dsRef) ) {
            dsRef = dConn.getDataSourceId();
            Txn.executeWrite(dConn.getDatasetGraph(), ()->dConn.getDatasetGraph().add(quad2) );
        }
        
        try ( DeltaConnection dConn = dClient.get(dsRef) ) {
            PatchLogInfo info = dConn.getPatchLogInfo();
            Id patchId = info.getLatestPatch();
            System.out.println("** fetch **");
            dLink.fetch(dsRef, patchId);
            System.out.println("** fetch **");
            System.out.println();
        }
        
        System.out.println("DONE");
        System.exit(0);
    }
    
    private static DeltaLink deltaLink(boolean httpServer, boolean register) {
        DeltaLink dLink;
        if ( httpServer ) {
            // Same process HTTP server.
            server(PORT, "DeltaServer");
            String URL = "http://localhost:"+PORT+"/";
            dLink = DeltaLinkHTTP.connect(URL);
        } else {
            // Local server
            LocalServer lServer = LocalServer.attach(Location.create("DeltaServer"));
            dLink = DeltaLinkLocal.connect(lServer);
        }

        if ( register ) {
            Id clientId = Id.create();
            dLink.register(clientId);
        }
        return dLink;
    }

    private static void server(int port, String base) {
            // --- Reset state.
    //        FileOps.clearAll("DeltaServer/ABC");
    //        FileOps.delete("DeltaServer/ABC");
        FileOps.clearAll(base);
        PatchLogServer dps = PatchLogServer.server(port, base);
        try { 
            dps.start();
        } catch(BindException ex) {
            Delta.DELTA_LOG.error("Address in use: port="+port);
            System.exit(0);
        }
    }
}
