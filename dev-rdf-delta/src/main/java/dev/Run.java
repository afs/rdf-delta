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

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.writer.WriterStreamRDFPlain ;
import org.apache.jena.sparql.core.DatasetGraph;
import org.seaborne.delta.Id;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.DataPatchServer;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;

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
    
    public static void main$() throws IOException {
        DataPatchServer server = DataPatchServer.server(1066, "DeltaServer");
        server.start();
        String URL = "http://localhost:1066/";
        
        DeltaLink dLink1 = DeltaLinkHTTP.connect(URL);
        dLink1.register(Id.create());
        Id dsref = dLink1.listDatasets().get(0);
        
        String s = dLink1.initialState(dsref);
        System.out.println(s);
        RDFDataMgr.parse(new WriterStreamRDFPlain(IO.wrapUTF8(System.out)), s);
        System.out.println("DONE");
        System.exit(0);
        
        

//        server.stop();
//
//        HttpClient hc = HttpOp.getDefaultHttpClient() ;
//        if ( hc instanceof CloseableHttpClient )
//            IO.close((CloseableHttpClient)hc) ;
//
//        HttpOp.setDefaultHttpClient(HttpClients.createDefault());
//
//        server = null ;
//        System.out.println();
//
//        DataPatchServer server2 = DataPatchServer.server(1066, "DeltaServer");
//        server2.start();
        
        //RDFPatch patch = RDFPatchOps.nullPatch(); 
        RDFPatch patch = RDFPatchOps.emptyPatch(); 
        //RDFPatch patch = RDFPatchOps.onePatch(); 

        System.out.println();
        // No registration check on DeltaLinkLocal
        dLink1.sendPatch(dsref, patch);
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
