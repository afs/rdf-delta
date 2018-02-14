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

package org.seaborne.delta.examples;

import java.net.BindException ;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.query.Dataset ;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.system.Txn ;
import org.seaborne.delta.Id ;
import org.seaborne.delta.client.DeltaClient ;
import org.seaborne.delta.client.DeltaConnection ;
import org.seaborne.delta.client.DeltaLinkHTTP ;
import org.seaborne.delta.client.LocalStorageType ;
import org.seaborne.delta.client.SyncPolicy ;
import org.seaborne.delta.client.Zone ;
import org.seaborne.delta.link.DeltaLink ;
import org.seaborne.delta.server.http.PatchLogServer;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.RDFPatchOps ;

/**
 * This example shows changes to a dataset being sent to a patch log server. For
 * convenience of this example, the patch log server is in the same JVM as the
 * dataset; it shares nothing with the local dataset. In a real world
 * deployment, the patch log server would be on another machine. All interaction
 * is over HTTP.
 */
public class DeltaEx4_DatasetToPatchLogServer {
    static { LogCtl.setJavaLogging(); }
    
    final static int PLOG_PORT = 1066;
    final static String PLOG_DIR = "DeltaServer";
    final static String Zone_DIR = "Zone";
    final static String DS_NAME = "ABCD";

    public static void main(String ...args) {
        try { main2(args) ; }
        finally { System.exit(0); }
    }
        
    public static void main2(String ...args) {
        //-- Setup a PatchLogServer
        // Ensure its work area exists and is empty.
        FileOps.exists(PLOG_DIR);
        FileOps.clearAll(PLOG_DIR);
        
        PatchLogServer patchLogServer = PatchLogServer.server(PLOG_PORT, PLOG_DIR);
        try { patchLogServer.start(); }
        catch (BindException ex) {
            System.err.println("Can't start the patch log server: "+ex.getMessage());
            System.exit(1);
        }

        // -- Setup connection to the patch log server.
        DeltaLink dLink = DeltaLinkHTTP.connect("http://localhost:"+PLOG_PORT+"/"); 
        dLink.register(Id.create());
        
        
        //-- Setup a Zone (workspace for recording local state like version of the dataset.
        // Ensure its work area exists and is empty.
        FileOps.exists(Zone_DIR);
        FileOps.clearAll(Zone_DIR);
        Zone zone = Zone.connect(Zone_DIR);
        DeltaClient dClient = DeltaClient.create(zone, dLink);
        
        // Create a patch log at the server
        Id dsRef = dClient.newDataSource(DS_NAME, "http://example/"+DS_NAME);
        
        // Create/attach the reference to a locally create TDB database.
        dClient.register(dsRef, LocalStorageType.TDB, SyncPolicy.TXN_RW);

        // "register" is these two steps:
        // dClient.attach(dsRef, LocalStorageType.TDB); 
        // dClient.connect(dsRef, TxnSyncPolicy.TXN_RW);
        
        // -- Use the dataset via a DeltaConnection.

        try( DeltaConnection dConn = dClient.get(dsRef) ) {
            Dataset ds = dConn.getDataset();
            Txn.executeWrite(ds,
                ()->RDFDataMgr.read(ds, "data.ttl")
                );
        }

        patchLogServer.stop();
        
        // -- Dump the patch.
        // The patch will be in "DeltaServer/ABCD/Log/patch-0001".
        System.out.println();
        RDFPatch patch = RDFPatchOps.read("DeltaServer/ABCD/Log/patch-0001");
        RDFPatchOps.write(System.out, patch);
        //Files.copy(Paths.get("DeltaServer/ABCD/Log/patch-0001"), System.out);
        System.exit(0);
    }
}
