/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.examples;

import java.net.BindException ;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.Pair ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.query.Dataset ;
import org.apache.jena.riot.Lang ;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.system.Txn ;
import org.seaborne.delta.DataSourceDescription ;
import org.seaborne.delta.Id ;
import org.seaborne.delta.client.DeltaClient ;
import org.seaborne.delta.client.DeltaConnection ;
import org.seaborne.delta.client.DeltaLinkHTTP ;
import org.seaborne.delta.client.LocalStorageType ;
import org.seaborne.delta.client.SyncPolicy ;
import org.seaborne.delta.client.Zone ;
import org.seaborne.delta.link.DeltaLink ;
import org.seaborne.delta.server.http.DeltaServer;

/**
 * This example shows changes to one dataset being sent to a patch log server
 * and synchronized with another dataset. For convenience of this example, the
 * datasets and the the patch log server is in the same JVM but nothing is
 * shared and all interaction is over HTTP.
 */
public class DeltaEx7_TwoDatasetsPatchLogServer {
    static { LogCtl.setJavaLogging(); }

    final static int PLOG_PORT = 1066;
    final static String PLOG_DIR = "DeltaServer";

    // Log name.
    final static String DS_NAME = "ABCD";

    // Two clients.
    final static String ZONE1_DIR = "Zone1";
    final static String ZONE2_DIR = "Zone2";

    public static void main(String ...args) {
        try { main2(args) ; }
        finally { System.exit(0); }
    }

    public static void main2(String ...args) {
        //-- Setup a PatchLogServer
        // Ensure its work area exists and is empty.
        FileOps.exists(PLOG_DIR);
        FileOps.clearAll(PLOG_DIR);

        DeltaServer server = DeltaServer.server(PLOG_PORT, PLOG_DIR);
        try { server.start(); }
        catch (BindException ex) {
            System.err.println("Can't start the patch log server: "+ex.getMessage());
            System.exit(1);
        }

        // -- Setup 2 datasets.
        String patchLogServerURL = "http://localhost:"+PLOG_PORT+"/";

        DeltaClient dClient1 = setup_dataset(DS_NAME, ZONE1_DIR, patchLogServerURL);
        DeltaClient dClient2 = setup_dataset(DS_NAME, ZONE2_DIR, patchLogServerURL);

        // Name of the new dataset patch log.
        Id dsRef = dClient2.getLink().getDataSourceDescriptionByName(DS_NAME).getId();

        // "register" is these two steps:
        // dClient.attach(dsRef, LocalStorageType.TDB);
        // dClient.connect(dsRef, TxnSyncPolicy.TXN_RW);

        // -- Use the dataset via a DeltaConnection.

        try( DeltaConnection dConn1 = dClient1.get(dsRef) ) {
            Dataset ds = dConn1.getDataset();
            Txn.executeWrite(ds,
                ()->RDFDataMgr.read(ds, "data.ttl")
                );
        }

        // read the other dataset
        try( DeltaConnection dConn2 = dClient2.get(dsRef) ) {
            Dataset ds = dConn2.getDataset();
            Txn.executeRead(ds, ()->RDFDataMgr.write(System.out, ds, Lang.TRIG) );
        }

        server.stop();
        System.exit(0);
    }

    private static DeltaClient setup_dataset(String dsName, String zoneDir, String patchLogServerURL) {
        DeltaLink dLink = DeltaLinkHTTP.connect(patchLogServerURL);

        // Probe to see if it exists.
        DataSourceDescription dsd = dLink.getDataSourceDescriptionByName(dsName);

        FileOps.exists(zoneDir);
        FileOps.clearAll(zoneDir);
        Zone zone = Zone.connect(zoneDir);
        DeltaClient dClient = DeltaClient.create(zone, dLink);

        // Get the Id.
        Id dsRef;
        if ( dsd == null )
            dsRef = dClient.newDataSource(DS_NAME, "http://example/"+DS_NAME);
        else
            dsRef = dsd.getId();
        // Create and setup locally.
        dClient.register(dsRef, LocalStorageType.TDB, SyncPolicy.TXN_RW);
        return dClient;
    }

    private static Pair<DeltaClient, Id> setup_dataset(String dsName, String patchLogServerURL) {

        return null ;
    }
}
