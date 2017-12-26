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
import org.apache.jena.system.Txn ;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.base.file.Location ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.Id;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.lib.DatasetGraphOneX;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.PatchLogServer ;
import org.seaborne.delta.server.local.DeltaLinkLocal ;
import org.seaborne.delta.server.local.LocalServer ;
import org.seaborne.delta.server.local.patchlog.PatchStore;
import org.seaborne.delta.server.local.patchlog.PatchStoreMem;
import org.seaborne.delta.server.local.patchlog.PatchStoreMgr;
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
        DatasetGraph dsg2 = RDFPatchOps.textWriter(dsg1, System.out);
        
        Txn.executeWrite(dsg1,  ()->{});
        Txn.executeWrite(dsg2,  ()->{});
    }
    
    public static void mainPatchStoreMem(String... args) throws IOException {
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
