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
            //.enableStats(true)
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
            JsonValue v = obj.getAsObject().get("datasets").getAsObject().get("/ds").getAsObject().get("endpoints").getAsObject().get("query") ;
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
        
        if ( false ) {
            LOG.info("Example 0") ;
            example0() ;
            try (QueryExecution qExec = 
                    QueryExecutionFactory.sparqlService("http://localhost:3330/ds/query", "SELECT * { ?s ?p ?o}") ) {
                QueryExecUtils.executeQuery(qExec); 
            }
            DataAccessPointRegistry.get().clear();
        }

        if ( false ) {
            LOG.info("Example 1") ;
            example1() ;
            try (QueryExecution qExec = 
                    QueryExecutionFactory.sparqlService("http://localhost:3333/ds/query", "SELECT * { ?s ?p ?o}") ) {
                QueryExecUtils.executeQuery(qExec); 
            }
            DataAccessPointRegistry.get().clear();
        }

        if ( false ) {
            LOG.info("Example 2") ;
            example2() ;
            try (QueryExecution qExec = 
                QueryExecutionFactory.sparqlService("http://localhost:3334/rdf/sparql", "SELECT * { ?s ?p ?o}") ) {
                QueryExecUtils.executeQuery(qExec); 
            }
            DataAccessPointRegistry.get().clear();
        }

        if ( false ) {
            LOG.info("Example 3") ;
            example3() ;

            Graph g = RDFDataMgr.loadGraph("D.trig");

            HttpEntity e = graphToHttpEntity(g) ;
            HttpOp.execHttpPut("http://localhost:3335/ds2/", e) ;

            try ( TypedInputStream in = HttpOp.execHttpGet("http://localhost:3335/ds2") ) {
                IO.copy(in, System.out) ;
            } catch (IOException ex) {
                ex.printStackTrace();
            }

            try (QueryExecution qExec = 
                QueryExecutionFactory.sparqlService("http://localhost:3335/ds/sparql", "SELECT * { ?s ?p ?o}") ) {
                QueryExecUtils.executeQuery(qExec); 
            }
            DataAccessPointRegistry.get().clear();
        }

        if ( false ) {
            LOG.info("Example 4") ;
            example4() ;

            Graph g = RDFDataMgr.loadGraph("D.trig");

            HttpEntity e = graphToHttpEntity(g) ;
            HttpOp.execHttpPut("http://localhost:3336/data", e) ;

            try ( TypedInputStream in = HttpOp.execHttpGet("http://localhost:3336/data") ) {
                IO.copy(in, System.out) ;
            } catch (IOException ex) {
                ex.printStackTrace();
            }

            try (QueryExecution qExec = 
                QueryExecutionFactory.sparqlService("http://localhost:3336/data", "SELECT * { ?s ?p ?o}") ) {
                QueryExecUtils.executeQuery(qExec); 
            }
            DataAccessPointRegistry.get().clear();
        }
        
        if ( true ) {
            LOG.info("Example 5") ;
            example5() ;

            Graph g = RDFDataMgr.loadGraph("D.trig");

            HttpEntity e = graphToHttpEntity(g) ;
            HttpOp.execHttpPut("http://localhost:3337/ds", e) ;

            try (QueryExecution qExec = 
                QueryExecutionFactory.sparqlService("http://localhost:3337/ds", "SELECT * { ?s ?p ?o}") ) {
                QueryExecUtils.executeQuery(qExec); 
            }
            DataAccessPointRegistry.get().clear();
        }

        System.out.println("DONE") ;
        System.exit(0) ;
    }
        
    /** Create an HttpEntity for the graph */  
    protected static HttpEntity graphToHttpEntity(final Graph graph) {

        final RDFFormat syntax = RDFFormat.TURTLE_BLOCKS ;
        ContentProducer producer = new ContentProducer() {
            @Override
            public void writeTo(OutputStream out) {
                RDFDataMgr.write(out, graph, syntax) ;
            }
        } ;

        EntityTemplate entity = new EntityTemplate(producer) ;
        String ct = syntax.getLang().getContentType().getContentType() ;
        entity.setContentType(ct) ;
        return entity ;
    }
    
    // Examples. 
    
    /** Create a SPARQL endpoint for an application dataset */ 
    private static void example0() {
        DatasetGraph dsg = dataset() ;
        // Run a Fuseki server with "/ds" as the dataset.
        // Default set up : query, update, graph store and quads operations. 
        FusekiEmbeddedServer.make(3330, "/ds", dsg).start() ;
    }
    
    /** Create a SPARQL endpoint for an application dataset */ 
    private static void example1() {
        DatasetGraph dsg = dataset() ;
        // Run a Fuseki server with "/ds" as the dataset.
        // Default set up : query, update, graph store and quads operations. 
        FusekiEmbeddedServer server = FusekiEmbeddedServer.create()
            .setPort(3333)
            .add("/ds", dsg) 
            .build() ;
        server.start() ;
    }

    /** Create a Fuseki server with a just a SPAQRL query endpoint.
     * The SPARQL endpoint URLs look like {@code /rdf/sparql?query=}
     */
    private static void example2() {
        DatasetGraph dsg = dataset() ;

        DataService queryService = new DataService(dsg) ;
        queryService.addEndpoint(OperationName.Query, "sparql");
        
        FusekiEmbeddedServer server = FusekiEmbeddedServer.create()
            .setPort(3334)
            .add("/rdf", queryService)
            .build() ;
        server.start() ;
        // Sync with the server - this is blocking.
        //server.join() ;
        //server.stop() ;
    }
    
    private static DatasetGraph dataset() {
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem() ;
        Txn.execWrite(dsg, ()->RDFDataMgr.read(dsg, "D.trig"));
        return dsg ;
    }

    /** Create a Fuseki server with two sets of services. One is the usual set of read-only endpoints,
     *  the other is just being able to do quads operations
     * GET, POST, PUT on  "/ds2" in N-quads and TriG.
     */
    private static void example3() {
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem() ;

        // A service with just being able to do quads operations
        // That is, GET, POST, PUT on  "/ds2" in N-quads and TriG. 
        DataService dataService = new DataService(dsg) ;
        dataService.addEndpoint(OperationName.Quads_RW, "");
        //dataService.addEndpoint(OperationName.Quads_RW, "quads");

        FusekiEmbeddedServer server = FusekiEmbeddedServer.create()
            .setPort(3335)
            .add("/ds", dsg, false) // read-only
            .add("/ds2", dataService)
            .build() ;
        server.start() ;
    }
    
    /** Create a Fuseki server with some services on the dataset URL. */
    private static void example4() {
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem() ;

        // A service with just being able to do quads operations
        // That is, GET, POST, PUT on  "/ds2" in N-quads and TriG. 
        DataService dataService = new DataService(dsg) ;
        dataService.addEndpoint(OperationName.Quads_RW, "");
        dataService.addEndpoint(OperationName.Query, "");
        dataService.addEndpoint(OperationName.Update, "");

        FusekiEmbeddedServer server = FusekiEmbeddedServer.create()
            .setPort(3336)
            .add("/data", dataService)
            .build() ;
        server.start() ;
    }
    
    /** Create a Fuseki server by reading a configuration file. */
    private static void example5() {
        FusekiEmbeddedServer server = FusekiEmbeddedServer.create()
            .setPort(3337)
            // Defines /ds
            .parseConfigFile("config.ttl")
            .build() ;
        server.start() ;
    }
}
