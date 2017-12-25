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

import org.apache.jena.atlas.lib.DateTimeUtils;
import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.graph.Graph;
import org.apache.jena.riot.web.HttpOp;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory ;
import org.apache.jena.sparql.core.Quad ;
import org.apache.jena.sparql.sse.SSE ;
import org.apache.jena.system.Txn ;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.base.file.Location ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo ;
import org.seaborne.delta.client.DeltaClient;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.client.LocalStorageType;
import org.seaborne.delta.client.TxnSyncPolicy;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.lib.DatasetGraphOneX;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.PatchLogServer ;
import org.seaborne.delta.server.local.DeltaLinkLocal ;
import org.seaborne.delta.server.local.LocalServer ;
import org.seaborne.delta.server.local.patchlog.PatchStore;
import org.seaborne.delta.server.local.patchlog.PatchStoreMem;
import org.seaborne.delta.server.local.patchlog.PatchStoreMgr;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.RDFPatchOps ;

public class Run {
    static { 
        //LogCtl.setLog4j(); 
        LogCtl.setJavaLogging();
    }

    static int PORT = 1068;
    
    public static void main(String... args) {
        //org.seaborne.delta.cmds.patchparse.main("/home/afs/tmp/X.rdfp");
        org.seaborne.delta.cmds.rdf2patch.main("/home/afs/tmp/D.ttl");
        System.exit(0);
        
//        FileOps.ensureDir("DB");
//        FileOps.clearAll("DB");
        DatasetGraph dsg = TDBFactory.createDatasetGraph();
        Txn.executeWrite(dsg,  ()->{});
        DatasetGraph dsg1 = new DatasetGraphOneX(dsg.getDefaultGraph());
        DatasetGraph dsg2 = RDFPatchOps.changesAsText(dsg1, System.out);
        
        Txn.executeWrite(dsg1,  ()->{});
        Txn.executeWrite(dsg2,  ()->{});
    }
    
    public static void mainMem(String... args) throws IOException {
        String DIR = "DeltaServer";
        FileOps.ensureDir(DIR);

        PatchLogServer patchServer = server(1066, DIR, true);
        System.out.println("-- --");
        PatchStore ps = new PatchStoreMem("mem");
        PatchStoreMgr.register(ps);
        PatchStoreMgr.setDftPatchStoreName("mem");
        patchServer.join();

//            PatchStore ps = new PatchStoreMem("mem");
//            DataSourceDescription dsd = new DataSourceDescription(Id.create(), "ABC", "http://example/");
//            ps.createLog(dsd, Paths.get(DIR));
//            ps.listDataSources();
//            ps.listPersistent(null);
//            System.out.println(ps.listDataSources());
//            System.out.println(ps.listPersistent(null));

        System.out.println("DONE");
        System.exit(0);
    }
    
    public static void mainOLD(String... args) throws IOException {
        boolean reset = true ;
        boolean embedded = true ;
        if ( reset ) {
            if ( embedded )
                FileOps.clearAll("DeltaServer");
            FileOps.clearAll("Zone");
        }
        Zone zone = Zone.create("Zone");
        
        DeltaLink dLink;
        if ( ! embedded ) {
            String URL = "http://localhost:"+PORT+"/";
            dLink = DeltaLinkHTTP.connect(URL);
            Id clientId = Id.create();
            dLink.register(clientId);
        } else {
            dLink = deltaLink(true, reset, true);
        }
        
        DeltaClient dc = DeltaClient.create(zone, dLink);
        
        String now = DateTimeUtils.nowAsString();
        Quad q = SSE.parseQuad("(_ :s :p '"+now+"')");
        if ( reset ) {
            Id dsRef0 = dc.newDataSource("ABC", "http://ex/ABC", LocalStorageType.MEM, TxnSyncPolicy.NONE);
        }
        Id dsRef = dc.connect("ABC", TxnSyncPolicy.NONE);
        
        try(DeltaConnection dConn = dc.get(dsRef)) {
            dConn.sync();
            DatasetGraph dsg = dConn.getDatasetGraph();
            Txn.executeWrite(dsg,  ()->dsg.add(q));
        }
        System.out.println("DONE");
        System.exit(0);

//        // Local setup.
//        LocalServer server = LocalServer.attach(Location.create("DeltaServer"));
//        DeltaLink dLink = DeltaLinkLocal.connect(server);
//        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
//        DeltaConnection.create(dataState, dsg, dLink, syncPolicy);
        
        
        //JenaSystem.DEBUG_INIT = true ;
        //DeltaSystem.DEBUG_INIT = true ;
        //DeltaSystem.init();
        try {
            //main$misc();
            mainPostPatch();
        } catch (Throwable ex) {
            System.out.println();
            System.out.flush();
            ex.printStackTrace();
            System.exit(1); }
        finally { System.exit(0); }
    }

    public static void mainPostPatch() throws IOException {
        FileOps.delete("DeltaServer/ABC");
        DeltaLink dLink = deltaLink(true, true, true);
        dLink.ping();
        Id dsRef = dLink.newDataSource("ABC", "http://example/ABC");

        RDFPatch patch = RDFPatchOps.emptyPatch();
        String x = RDFPatchOps.str(patch);
        
        String tok = dLink.getRegToken().asParam();
        
        HttpOp.execHttpPost("http://localhost:"+PORT+"/ABC?token="+tok, DeltaConst.contentTypePatchText, x);
        
//        //RDFPatch patch = RDFPatchOps.emptyPatch();
//        long version = dLink.append(dsRef, patch);
        long version = 1;
        RDFPatch patch2 = dLink.fetch(dsRef, Id.fromNode(patch.getId()));
    }
    
    public static void main$dc() throws IOException {
        FileOps.clearAll("Zone");
        Zone zone = Zone.create("Zone");
        
        Quad quad1 = SSE.parseQuad("(:g :s :p 111)");
        Quad quad2 = SSE.parseQuad("(:g :s :p 222)");
        Quad quad3 = SSE.parseQuad("(:g :s :p 333)");
        
        boolean httpServer = false;
        
        DeltaLink dLink = deltaLink(httpServer, true, true);
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

        // Quick run-through of some operations as a during develop, pre-test quick check.  
    public static void main$misc() throws IOException {
        // Do a delete.
        
        // --- Reset state.
        FileOps.clearAll("Zone");
        Zone zone = Zone.create("Zone");
        
        boolean httpServer = true;
        DeltaLink dLink = deltaLink(httpServer, true, true);
        
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
    
    private static DeltaLink deltaLink(boolean httpServer, boolean cleanStart, boolean register) {
        DeltaLink dLink;
        if ( httpServer ) {
            // Same process HTTP server.
            server(PORT, "DeltaServer", cleanStart);
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

    private static PatchLogServer server(int port, String base, boolean cleanStart) {
        if ( cleanStart )
            FileOps.clearAll(base);
        Location baseArea = Location.create(base);
        String configFile = baseArea.getPath(DeltaConst.SERVER_CONFIG);
        LocalServer server = LocalServer.create(baseArea, configFile);
        DeltaLink link = DeltaLinkLocal.connect(server);
        PatchLogServer dps = PatchLogServer.create(port, link);
        try { 
            dps.start();
            return dps;
        } catch(BindException ex) {
            Delta.DELTA_LOG.error("Address in use: port="+port);
            System.exit(0);
            return null;
        }
    }
}
