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

import java.io.IOException ;
import java.io.OutputStream ;

import org.apache.http.HttpEntity ;
import org.apache.http.entity.ContentProducer ;
import org.apache.http.entity.EntityTemplate ;
import org.apache.jena.atlas.json.JSON ;
import org.apache.jena.atlas.json.JsonObject ;
import org.apache.jena.atlas.json.JsonValue ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.atlas.web.TypedInputStream ;
import org.apache.jena.fuseki.Fuseki ;
import org.apache.jena.fuseki.embedded.FusekiEmbeddedServer ;
import org.apache.jena.fuseki.server.DataAccessPointRegistry ;
import org.apache.jena.fuseki.server.DataService ;
import org.apache.jena.fuseki.server.OperationName ;
import org.apache.jena.graph.Graph ;
import org.apache.jena.query.* ;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.RDFFormat ;
import org.apache.jena.riot.web.HttpOp ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.DatasetGraphFactory ;
import org.apache.jena.sparql.core.Quad ;
import org.apache.jena.sparql.sse.SSE ;
import org.apache.jena.sparql.util.QueryExecUtils ;
import org.apache.jena.system.Txn ;
import org.eclipse.jetty.util.IO ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

/** Create and run an Fuseki server inside an large application */

public class DevEmbeddedFuseki {
    
    // Canonical names.
    // Servlet attributes
    // Examples -> tests
    // enableStats()
    // 
    
    static {
        //LogCtl.setJavaLogging() ;
        LogCtl.setJavaLogging();
    }
    static Logger LOG = LoggerFactory.getLogger("main") ;
    
    public static void main(String...arg) {
        try { example6() ; System.exit(0); }
        catch(Throwable th) {
            th.printStackTrace(System.err) ;
            System.exit(10);
        }
    }
     
    public static void example6() {
        LogCtl.setJavaLogging();
        Logger LOG = LoggerFactory.getLogger("example6") ;
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem() ;
        DatasetGraph dsg1 = DatasetGraphFactory.createTxnMem() ;
        // Run a Fuseki server with "/ds" as the dataset.
        // Default set up : query, update, graph store and quads operations. 
        //FusekiEmbeddedServer server = FusekiEmbeddedServer.make(3330, "/ds", dsg) ;
        
        FusekiEmbeddedServer server = FusekiEmbeddedServer.create()
            .setPort(3330)
            .setContextPath("/ABC")
            .add("/ds", dsg)
            .enableStats(true)
            .build() ;
            
        server.start() ;
        
        
        
        LOG.info("Remote 1") ;
        try (QueryExecution qExec = 
            QueryExecutionFactory.sparqlService("http://localhost:3330/ABC/ds/query", "SELECT * { ?s ?p ?o}") ) {
            QueryExecUtils.executeQuery(qExec); 
        }
        
        Txn.execWrite(dsg,  ()->{
            Quad q = SSE.parseQuad("(_ :s :p _:b)") ;
            dsg.add(q); 
        }) ;
        
        LOG.info("Remote 2") ;
        try (QueryExecution qExec = 
            QueryExecutionFactory.sparqlService("http://localhost:3330/ABC/ds/query", "SELECT * { ?s ?p ?o}") ) {
            QueryExecUtils.executeQuery(qExec); 
        }

//        LOG.info("Local stats") ;
//        JsonObject obj = ActionStats.generateStats(DataAccessPointRegistry.get()) ;
//        //JSON.write(obj);
//        JsonValue v = obj.getObj("datasets").getObj("/ds").getObj("endpoints").getObj("query") ;
//        JSON.write(v);
//        System.out.println();
        
        LOG.info("Remote stats") ;
        try ( TypedInputStream in = HttpOp.execHttpGet("http://localhost:3330/ABC/$/stats") ) {
            //IO.copy(in, System.out) ;
            JsonObject obj = JSON.parse(in) ;
            //JsonValue v = obj.getObj("datasets").getObj("/ds").getObj("endpoints").getObj("query") ;
            JsonValue v = obj.getAsObject().getObj("datasets").getObj("/ds").getObj("endpoints").get("query") ;
            JSON.write(v);
        }
        
        server.stop() ;
    }
    
    public static void main2() {
        
        if ( false ) {
            // NCSA logging
            java.util.logging.Logger rLog = java.util.logging.Logger.getLogger(Fuseki.requestLogName) ;
//            Handler[] h = rLog.getHandlers() ;
//            rLog.setUseParentHandlers(false);
//            System.err.println(h.length);
//            System.err.println(h[0].getClass().getSimpleName()) ;
//            h[0].setLevel(java.util.logging.Level.INFO);
            // Regular Fuseki logging. 
            java.util.logging.Logger.getLogger(Fuseki.actionLogName).setLevel(java.util.logging.Level.OFF) ;
            // Server start up.
            java.util.logging.Logger.getLogger(Fuseki.serverLogName).setLevel(java.util.logging.Level.OFF) ;
        }
    }        
}
