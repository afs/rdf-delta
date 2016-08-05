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

import embedded.FusekiEmbeddedServer ;
import org.apache.http.HttpEntity ;
import org.apache.http.entity.ContentProducer ;
import org.apache.http.entity.EntityTemplate ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.atlas.web.TypedInputStream ;
import org.apache.jena.fuseki.server.DataService ;
import org.apache.jena.fuseki.server.OperationName ;
import org.apache.jena.graph.Graph ;
import org.apache.jena.query.QueryExecution ;
import org.apache.jena.query.QueryExecutionFactory ;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.RDFFormat ;
import org.apache.jena.riot.web.HttpOp ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.DatasetGraphFactory ;
import org.apache.jena.sparql.util.QueryExecUtils ;
import org.eclipse.jetty.util.IO ;

/** Create and run an Fuseki server inside an large application */

public class DevEmbeddedFuseki {
    
    static {
        //LogCtl.setJavaLogging() ;
        LogCtl.setJavaLogging();
    }

    public static void main(String...arg) {
        try { main2() ; }
        catch(Throwable th) {
            th.printStackTrace(System.err) ;
            System.exit(10);
        }
    }
        
    public static void main2() {
        
        // ------------------------
        
        // Tests
        if ( false ) {
            example1() ;
            try (QueryExecution qExec = 
                    QueryExecutionFactory.sparqlService("http://localhost:3333/ds/query", "SELECT * { ?s ?p ?o}") ) {
                QueryExecUtils.executeQuery(qExec); 
            }
        }

        if ( false ) {
            example2() ;
            try (QueryExecution qExec = 
                QueryExecutionFactory.sparqlService("http://localhost:3334/rdf/sparql", "SELECT * { ?s ?p ?o}") ) {
                QueryExecUtils.executeQuery(qExec); 
            }
        }

        if ( false ) {
            example3() ;

            Graph g = RDFDataMgr.loadGraph("D.trig");

            HttpEntity e = graphToHttpEntity(g) ;
            HttpOp.execHttpPut("http://localhost:3335/ds2/quads", e) ;

            try ( TypedInputStream in = HttpOp.execHttpGet("http://localhost:3335/ds2") ) {
                IO.copy(in, System.out) ;
            } catch (IOException ex) {
                ex.printStackTrace();
            }

            try (QueryExecution qExec = 
                QueryExecutionFactory.sparqlService("http://localhost:3335/ds/sparql", "SELECT * { ?s ?p ?o}") ) {
                QueryExecUtils.executeQuery(qExec); 
            }
        }

        if ( true ) {
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
        }

        System.out.println("DONE") ;
        System.exit(0) ;
    }
        
    /** Create an HttpEntity for the graph */  
    protected static HttpEntity graphToHttpEntity(final Graph graph) {

        final RDFFormat syntax = RDFFormat.NQ ;
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
    private static void example1() {
        // Setup a dataset. Put some data in it.
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem() ;
        RDFDataMgr.read(dsg, "D.trig");

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
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem() ;
        RDFDataMgr.read(dsg, "D.trig");

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
            //.add("/ds", dsg, false) // read-only
            .add("/ds2", dataService)
            .build() ;
        server.start() ;
    }
    
    /** Create a Fuseki server with some services on the datset URL.
     */
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

}
