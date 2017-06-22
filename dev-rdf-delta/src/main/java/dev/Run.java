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
import java.util.List;

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
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.DataPatchServer ;
import org.seaborne.delta.server.local.DeltaLinkLocal ;
import org.seaborne.delta.server.local.LocalServer ;

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
            main$();
        } catch (Throwable ex) {
            System.out.println();
            System.out.flush();
            ex.printStackTrace();
            System.exit(1); }
        finally { System.exit(0); }
    }

    public static void main$() throws IOException {
        // --- Reset state.
        FileOps.clearAll("Zone");
        Zone.get().init("Zone");
        
        DeltaLink dLink1 = null;
        boolean httpServer = true;
        
        if ( httpServer ) {
            // Same process HTTP server.
            server(PORT, "DeltaServer");
            String URL = "http://localhost:"+PORT+"/";
            dLink1 = DeltaLinkHTTP.connect(URL);
        } else {
            // Local server
            LocalServer lServer = LocalServer.attach(Location.create("DeltaServer"));
            dLink1 = DeltaLinkLocal.connect(lServer);
        }
        
        Id clientId = Id.create();
        dLink1.register(clientId);
        
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
        Zone zone = Zone.get();
        //Id datasourceId = dLink1.newDataSource(datasourceName, datasourceURI);
        //DataState dataState = zone.create(datasourceId, datasourceName, datasourceURI, Backing.TDB);

        
        try ( DeltaConnection dConn = DeltaConnection.create(zone, datasourceName, datasourceURI, dsg, dLink1) ) {}
        System.out.println();
        
//        try ( DeltaConnection dConn = DeltaConnection.create(zone, datasourceName, datasourceURI, dsg, dLink1) ) {
//            String x = dConn.getInitialStateURL();
//            Txn.executeWrite(dConn.getDatasetGraph(), ()->RDFDataMgr.read(dConn.getDatasetGraph(), x));
//        }
        
        Id dsRef = dLink1.getDataSourceDescription(datasourceURI).getId();
        System.out.println();
        
//        try ( DeltaConnection dConn = DeltaConnection.create(Zone.get(), clientId, "ABC", "http://example/ABC", dsg, dLink1) ) {
//            dConn.sync();
//            dsRef = dConn.getDataSourceId();
//            Txn.executeWrite(dConn.getDatasetGraph(), ()->dConn.getDatasetGraph().add(quad1) );
//        }
        
        
        try ( DeltaConnection dConn = DeltaConnection.connect(Zone.get(), dsRef, dsg, dLink1)) {
            dsRef = dConn.getDataSourceId();
            Txn.executeWrite(dConn.getDatasetGraph(), ()->dConn.getDatasetGraph().add(quad2) );
        }
        
        try ( DeltaConnection dConn = DeltaConnection.connect(Zone.get(), dsRef, dsg, dLink1)) {
            PatchLogInfo info = dConn.getPatchLogInfo();
            Id patchId = info.getLatestPatch();
            System.out.println("** fetch **");
            dLink1.fetch(dsRef, patchId);
            System.out.println("** fetch **");
            System.out.println();
        }
        
//        DataState dataState = Zone.get().attach(dsRef);
//        System.out.println();
//        System.out.println("Data State : "+dataState);
//        System.out.println();

//        DatasetGraph dsg1 = DatasetGraphFactory.createTxnMem();
//        try ( DeltaConnection dConn = DeltaConnection.attach(Zone.get(), dsRef, dsg1, dLink1) ) {
//            RDFDataMgr.write(System.out, dsg1, Lang.TRIG);
//        }

//        
//        Id dsref = dLink1.listDatasets().get(0);
//        String s = dLink1.initialState(dsref);
//        System.out.println(s);
//        RDFDataMgr.parse(new WriterStreamRDFPlain(IO.wrapUTF8(System.out)), s);

        System.out.println("DONE");
        System.exit(0);
    }
    
    private static void server(int port, String base) {
            // --- Reset state.
    //        FileOps.clearAll("DeltaServer/ABC");
    //        FileOps.delete("DeltaServer/ABC");
        FileOps.clearAll(base);
        DataPatchServer dps = DataPatchServer.server(port, base);
        try { 
            dps.start();
        } catch(BindException ex) {
            Delta.DELTA_LOG.error("Address in use: port="+port);
            System.exit(0);
        }
    }

    // Initial state.
    static DeltaConnection xconnectOrCreate(Zone zone, Id clientId, String datasourceName, DatasetGraph dsg0, DeltaLink dLink, boolean create) {
        if ( create ) {
            DeltaConnection dConn = DeltaConnection.create(zone, datasourceName, "http://example/new", dsg0, dLink);
            Id dsRef = dConn.getDataSourceId();
            System.out.println("++++ Create: "+dsRef);
            return dConn;
        } else {
            List<Id> datasources = dLink.listDatasets();
            System.out.println("++++ "+datasources);
            Id dsRef = datasources.get(0);
            DeltaConnection dConn = DeltaConnection.connect(zone, dsRef, dsg0, dLink);
            return dConn;
        }
    }

    public static void example() {
            String URL = "http://localhost:"+PORT+"/";
            Id clientId = Id.create();
    
            DeltaLink dLink = DeltaLinkHTTP.connect(URL);
            dLink.register(clientId);
    
            // Find the dataset.
            List<Id> datasources = dLink.listDatasets();
            Id dsRef = datasources.get(0);
            System.out.printf("dsRef = %s\n", dsRef);
    
            DatasetGraph dsg0 = DatasetGraphFactory.createTxnMem();
    
            try ( DeltaConnection dConn = DeltaConnection.connect(Zone.get(), dsRef, dsg0, dLink)) {
            
    //        // Work with this dataset:
    //        DatasetGraph dsg = dConn.getDatasetGraph();
    //        Txn.executeWrite(dsg, ()->
    //            RDFDataMgr.read(dsg, "some_data.ttl")
    //        );
    //        dsg.begin(ReadWrite.WRITE);
    //        try {
    //            RDFDataMgr.read(dsg, "some_data.ttl");
    //            dsg.commit();
    //        } finally {
    //            dsg.end();
    //        }
        }
    }
}
