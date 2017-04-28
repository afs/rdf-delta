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
import java.util.List;

import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory ;
import org.apache.jena.system.Txn ;
import org.seaborne.delta.Id;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.DataPatchServer;

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
    
    public static void main(String... args) throws IOException {
        try {
            main$();
        } catch (Throwable ex) {
            System.out.println();
            System.out.flush();
            ex.printStackTrace();
            System.exit(1); }
    }

    public static void example() {
        String URL = "http://localhost:1066/";
        Id clientId = Id.create();

        DeltaLink dLink = DeltaLinkHTTP.connect(URL);
        dLink.register(clientId);

        // Find the dataset.
        List<Id> datasources = dLink.listDatasets();
        Id dsRef = datasources.get(0);
        System.out.printf("dsRef = %s\n", dsRef);

        DatasetGraph dsg0 = DatasetGraphFactory.createTxnMem();

        try ( DeltaConnection dConn = DeltaConnection.connect(Zone.get(), clientId, dsRef, dsg0, dLink)) {
        
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
    
    public static void main$() throws IOException {
        // Server
        DataPatchServer server = DataPatchServer.server(1066, "DeltaServer");
        server.start();
        String URL = "http://localhost:1066/";
        // Zone
        Zone.get().init("Zone");
        //Client
        DeltaLink dLink1 = DeltaLinkHTTP.connect(URL);
        Id clientId = Id.create();
        dLink1.register(clientId);
        
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        
        try ( DeltaConnection dConn = DeltaConnection.create(Zone.get(), clientId, "ABC", "http://example/ABC", dsg, dLink1) ) {
            Txn.executeWrite(dConn.getDatasetGraph(), 
                             ()->RDFDataMgr.read(dConn.getDatasetGraph().getDefaultGraph(), "D.ttl"));
                             
        }
//        
//        Id dsref = dLink1.listDatasets().get(0);
//        String s = dLink1.initialState(dsref);
//        System.out.println(s);
//        RDFDataMgr.parse(new WriterStreamRDFPlain(IO.wrapUTF8(System.out)), s);

        System.out.println("DONE");
        System.exit(0);
    }
    
    static DeltaConnection connectOrCreate(Zone zone, Id clientId, String datasourceName, DatasetGraph dsg0, DeltaLink dLink, boolean create) {
        if ( create ) {
            DeltaConnection dConn = DeltaConnection.create(zone, clientId, datasourceName, "http://example/new", dsg0, dLink);
            Id dsRef = dConn.getDatasourceId();
            System.out.println("++++ Create: "+dsRef);
            return dConn;
        } else {
            List<Id> datasources = dLink.listDatasets();
            System.out.println("++++ "+datasources);
            Id dsRef = datasources.get(0);
            DeltaConnection dConn = DeltaConnection.connect(zone, clientId, dsRef, dsg0, dLink);
            return dConn;
        }
    }
}
