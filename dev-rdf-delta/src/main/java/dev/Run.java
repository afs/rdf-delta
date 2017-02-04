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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

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
import org.seaborne.delta.server.local.*;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;

public class Run {
    static { 
        //LogCtl.setLog4j(); 
        LogCtl.setJavaLogging();
    }
    
    // Local.
    // Local cache / HTTP only
    
    // Tests for restart
    // Need patch rejection
    
    // PatchLog - conflates "index" and "version" - acceptable?
    //          - revisit HistoryEntry - it keeps patches? LRU cache of patches.  
    // Look for [DISK]
    
    public static void main1(String... args) throws IOException {
        Id dsRef = Id.create();
        PatchLog patchLog = PatchLog.attach(dsRef, Location.create("Patches"));
        
        patchLog.getFileStore().getIndexes().forEach(idx->System.out.printf("idx=%d\n", idx));
        
        patchLog.getFileStore().getIndexes().forEach(idx->{
            RDFPatch patch = patchLog.fetch(idx);
        });
        
        // Find latest.
        Id latest = patchLog.getLatestId();
        int version = patchLog.getLatestVersion();
        System.out.printf("Latest: %d : %s\n", version, latest);
        System.out.println();
        
        RDFPatch rdfPatch = RDFPatchOps.fileToPatch("Patches/test");
        patchLog.validate(rdfPatch);
        
//        int version2 = ps.getLatestVersion();
//        System.out.printf("Latest: %d\n", version2);
        
        System.out.println("DONE");
        System.exit(0);
    }
    
    public static void main(String... args) throws IOException {
        boolean cleanServer = false;
        String datasourceName = "XYZ";
        Location loc = Location.create("DeltaServer");
        if ( cleanServer )
            FileOps.clearAll(loc.getPath(datasourceName));

        LocalServer server = LocalServer.attach(loc);
        List<DataSource> x = server.listDataSources();
        
        DeltaLink dLink = DeltaLinkLocal.connect(server);
        
        Id clientId = Id.create();
        dLink.register(clientId);
        Id dsRef;
        if ( cleanServer ) {
            // Predates DeltaConnection.create
            dLink.register(clientId);
            dsRef = dLink.newDataSource(datasourceName, "http://example/new");
            dLink.deregister();
            
            String FN = "DeltaServer/XYZ/source.cfg";
            if ( ! Files.exists(Paths.get(FN)) )
                System.err.println("No file!");
            
        } else {
            dsRef = dLink.listDatasets().get(0);
        }
        DatasetGraph dsg0 = DatasetGraphFactory.createTxnMem();

        DeltaConnection dConn = DeltaConnection.connect("ConnectionToXYZ", clientId, dsRef, dsg0, dLink);

        DeltaConnectionState dcs = new DeltaConnectionState(dConn, dsRef, Location.create("DConn"));
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
