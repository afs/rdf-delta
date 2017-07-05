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

import static org.seaborne.delta.DeltaConst.symDeltaClient ;
import static org.seaborne.delta.DeltaConst.symDeltaConnection ;
import static org.seaborne.delta.DeltaConst.symDeltaZone ;

import java.net.BindException ;

import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.lib.Timer ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.fuseki.embedded.FusekiEmbeddedServer ;
import org.apache.jena.query.QueryExecution ;
import org.apache.jena.query.ResultSetFormatter ;
import org.apache.jena.rdfconnection.RDFConnection ;
import org.apache.jena.rdfconnection.RDFConnectionFactory ;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.system.Txn ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.PatchLogInfo ;
import org.seaborne.delta.client.DeltaClient ;
import org.seaborne.delta.client.DeltaConnection ;
import org.seaborne.delta.client.Zone ;
import org.seaborne.delta.server.http.PatchLogServer ;

public class RunAll {

    static { 
        //LogCtl.setLog4j(); 
        LogCtl.setJavaLogging();
    }
    
    public static void main(String... args) {
        try {
            Timer timer = new Timer();
            timer.startTimer();
            main$();
            long x = timer.endTimer();
            System.out.println();
            System.out.printf("Time = %,.3fs\n", x/1000.0);
        } catch (Throwable ex) {
            System.out.println();
            System.out.flush();
            ex.printStackTrace();
            System.exit(1); }
        finally { System.exit(0); }
    }
    
    public static void main$(String... args) {
        FileOps.ensureDir("Zone1");
        FileOps.clearAll("Zone1");
        FileOps.ensureDir("Zone2");
        FileOps.clearAll("Zone2");

        Delta.init();
        
        String fuseki_conf1 = "../rdf-delta-test/testing/fuseki_conf_1.ttl"; 
        String fuseki_conf2 = "../rdf-delta-test/testing/fuseki_conf_2.ttl";
        
        int F1_PORT = 3050 ;
        int F2_PORT = 3060 ;
        
        int D_PORT = 1068;
        
        PatchLogServer dps = server(D_PORT, "DeltaServerFuseki");
        String URL = "http://localhost:"+D_PORT+"/";       
        
        FusekiEmbeddedServer server1 = FusekiEmbeddedServer.create()
            .setPort(F1_PORT).parseConfigFile(fuseki_conf1).build();
        // Fire up a second server.  Only starts if DPS running. -- FIXME
        FusekiEmbeddedServer server2 = FusekiEmbeddedServer.create()
            .setPort(F2_PORT).parseConfigFile(fuseki_conf2).build();

        DatasetGraph dsg1 = server1.getDataAccessPointRegistry().get("/ds1").getDataService().getDataset();
        DatasetGraph dsg2 = server2.getDataAccessPointRegistry().get("/ds2").getDataService().getDataset();
        server1.start();
        server2.start();
        
        Zone zone1 = (Zone)dsg1.getContext().get(symDeltaZone);
        Zone zone2 = (Zone)dsg2.getContext().get(symDeltaZone);
        
        String PREFIX = "PREFIX : <http://example/>\n";
        
        // Update to fuseki.
        try (RDFConnection conn1 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+"/ds1") ) {
            conn1.update(PREFIX+"INSERT DATA { :s :p 1804 }");
        }
        
        
        RDFConnection conn1 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+"/ds1") ;
        //RDFConnection conn2 = RDFConnectionFactory.connect("http://localhost:"+F2_PORT+"/ds2") ;
        
        if ( true ) {
            //dps.stop();
            RDFConnection conn2 = RDFConnectionFactory.connect("http://localhost:"+F2_PORT+"/ds2");
            // server2.
            System.out.println("** server2");
            
            try ( QueryExecution qExec = conn2.query("SELECT * { ?s ?p ?o }") ) { 
                ResultSetFormatter.out(qExec.execSelect());
            }
            
            conn2.update(PREFIX+"INSERT DATA { :s :p 1816 }");
            
            try ( QueryExecution qExec = conn2.query("SELECT * { ?s ?p ?o }") ) { 
                ResultSetFormatter.out(qExec.execSelect());
            }
            
            System.out.println();
            System.out.println("** server1");
            try ( QueryExecution qExec = conn1.query("SELECT * { ?s ?p ?o }") ) { 
                ResultSetFormatter.out(qExec.execSelect());
            }
            
            System.out.println();
            System.out.println("DONE/Fuseki^2");
            return;
        }
        
        //dps.stop();

        if ( false ) {
            // Update dataset.
            DeltaClient dClient = (DeltaClient)dsg1.getContext().get(symDeltaClient);
            DeltaConnection dConn = (DeltaConnection)dsg1.getContext().get(symDeltaConnection);
            Zone zone = (Zone)dsg1.getContext().get(symDeltaZone);
            
            PatchLogInfo info1 = dConn.getPatchLogInfo();
            status(dConn);
            
            Txn.executeWrite(dsg1, ()->RDFDataMgr.read(dsg1, "D1.ttl"));
            //Txn.executeRead(dsg, ()->RDFDataMgr.write(System.out, dsg, Lang.TRIG));
    
            PatchLogInfo info2 = dConn.getPatchLogInfo();
            status(dConn);
            // Connect Fuseki2.
        }
        
        
        
        
        System.out.println("DONE");
    }

    private static void status(DeltaConnection dConn) {
        PatchLogInfo info1 = dConn.getPatchLogInfo();
        System.out.println(dConn);
        System.out.println(info1);
    }
    
    private static PatchLogServer server(int port, String base) {
        // --- Reset state.
        FileOps.ensureDir(base);
//        FileOps.clearAll("DeltaServer/ABC");
//        FileOps.delete("DeltaServer/ABC");
        FileOps.clearAll(base);
        PatchLogServer dps = PatchLogServer.server(port, base);
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
