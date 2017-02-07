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

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.Id;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.LocalServer;

public class Run {
    static { 
        //LogCtl.setLog4j(); 
        LogCtl.setJavaLogging();
    }
    
    // Tests for restart
    
    // Remove (hide) DSG from DeltaConnection start-up : TDB or file only.
    
    // ** Need patch rejection
    // ** Start at id:nil (or id:datasource?)
    
    // TransPBlob not PersistentState ; memory versions.
    
    // [PersistentVersion] in client 
    //   DeltaLink? DeltaConnection? 
    //   Persistent counter, integritity of the log and client side. 
    
    // PatchCache
    
    // Client side persistence.
    // Client side shadow.
    // DataState init

    /*
18:04:55 INFO  Delta                : Patch range [4, 4]
18:04:55 INFO  Delta                : Sync: patch=4

Don't read 4!
     */
    
    public static void main(String... args) throws IOException {
        boolean cleanServer = false;
        String datasourceName = "XYZ";
        Location serverLoc = Location.create("DeltaServer");
        Location dConnLoc = Location.create("DConn");
        
        if ( cleanServer ) {
            FileOps.clearAll(dConnLoc.getDirectoryPath());
            FileOps.clearAll(serverLoc.getPath(datasourceName));
            FileOps.delete(serverLoc.getPath(datasourceName));
        }

        LocalServer server = LocalServer.attach(serverLoc);
        
        // --------
        //List<DataSource> x = server.listDataSources();
        DeltaLink dLink = DeltaLinkLocal.connect(server);
        
        Id clientId = Id.create();
        //dLink.register(clientId);
        
        DatasetGraph dsg0 = DatasetGraphFactory.createTxnMem();
        Id dsRef;
        DeltaConnection dConn;
         
        
        if ( cleanServer ) {
            dConn = DeltaConnection.create(clientId, dConnLoc, datasourceName, "http://example/new", dsg0, dLink);
//            String FN = "DeltaServer/XYZ/source.cfg";
//            if ( ! Files.exists(Paths.get(FN)) )
//                System.err.println("No file!");
            dsRef = dConn.getDatasourceId();
        } else {
            dsRef = dLink.listDatasets().get(0);
            dConn = DeltaConnection.connect(clientId, dConnLoc, dsRef, dsg0, dLink);
        }
        
        
        
//        DataState dcs = new DataState(dConn, dsRef, Location.create("DConn"));
        dConn.sync();
        
        DatasetGraph dsg = dConn.getDatasetGraph();
        Txn.executeWrite(dsg,  ()->{
            Quad q = SSE.parseQuad("(_ :ss :pp :oo)");
            dsg.add(q);
        });
        
        
        
        //dLink.listDatasets();
        //Must register to create.
//        
//        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
//        Location localData = null;
//        
//        
//        //DeltaConnection dConn1 = DeltaConnection.create(clientId, dsRef, localData, dLink);
//        
//   
//        // Shoduk be rejected if it does not exist.
//        DeltaConnection dConn2 = DeltaConnection.connect("ConnectionToXYZ", clientId, dsRef, dsg, dLink);
//        dConn2.sync();
        
        System.out.println("DONE");
        System.exit(0);
    }
        
    
    
}
