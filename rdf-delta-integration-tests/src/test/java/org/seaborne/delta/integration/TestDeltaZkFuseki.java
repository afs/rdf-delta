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

package org.seaborne.delta.integration;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.seaborne.delta.systemtest.Matrix.deltaServerURL1;
import static org.seaborne.delta.systemtest.Matrix.deltaServerURL2;

import java.util.function.Supplier;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.graph.Node;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.sparql.sse.SSE;
import org.junit.*;
import org.seaborne.delta.Id;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.client.DeltaLinkSwitchable;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.systemtest.Matrix;

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class TestDeltaZkFuseki {

    private static Node g1 = SSE.parseNode(":g1"); 
    private static Node g2 = SSE.parseNode("_:g2"); 
    private static Node s1 = SSE.parseNode(":s1"); 
    private static Node s2 = SSE.parseNode("_:s2"); 
    private static Node p1 = SSE.parseNode(":p1"); 
    private static Node p2 = SSE.parseNode(":p2"); 
    private static Node o1 = SSE.parseNode("<http://example/o1>");
    private static Node o2 = SSE.parseNode("123");
    
    @BeforeClass public static void beforeClass() { DeltaLinkSwitchable.silentSwitchOver = true; } 
    @AfterClass public static void afterClass()   { DeltaLinkSwitchable.silentSwitchOver = false; } 
    
    @Before public void before() { Matrix.setup(); }
    @After  public void after()  { Matrix.teardown(); }

    // Fuseki switch over tests.
    // Two DeltaServers, one Zookeeper.
    
    // Things only happen on DeltaConnection.sync.

    // Single fuseki server, two Delta servers 
    @Test
    public void t1_fusekiOne() {
        DeltaLink dLink1 = DeltaLinkHTTP.connect(deltaServerURL1);
        DeltaLink dLink2 = DeltaLinkHTTP.connect(deltaServerURL2);
        dLink1.ping();
        dLink2.ping();

        assertFalse(dLink1.existsByName("ABC"));
        Id logABC = dLink1.newDataSource("ABC", "http://example/abc");
        Lib.sleep(500);
        assertTrue(dLink1.existsByName("ABC"));
        assertTrue(dLink2.existsByName("ABC"));

        int fusekiPort = Matrix.choosePort();
        DeltaLinkSwitchable dLinkFuseki = (DeltaLinkSwitchable)Matrix.setupFuseki("ABC", "target/Zone", fusekiPort, deltaServerURL1, deltaServerURL2);
        String fusekiURL = "http://localhost:"+fusekiPort+"/ABC";
        RDFConnection conn = RDFConnectionFactory.connect(fusekiURL);

        conn.update("PREFIX : <http://delta/> INSERT DATA { :s :p :o }");
        assertCount("Count, before failover", conn, 1);

        if ( true )
            Matrix.deltaServer1.stop();
        else
            dLinkFuseki.switchover();

        assertCount("Count, after failover", conn, 1);
        conn.update("PREFIX : <http://delta/> INSERT DATA { :s :p :o2 }");
        assertCount(conn, 2);
    }

    @Test
    public void t2_fuseki2() {
        DeltaLink dLink1 = DeltaLinkHTTP.connect(deltaServerURL1);
        DeltaLink dLink2 = DeltaLinkHTTP.connect(deltaServerURL2);
        dLink1.ping();
        dLink2.ping();
        
        if ( dLink1.existsByName("ABC") ) {
            System.err.println("Exists");
        }
        
        Id log2 = dLink2.newDataSource("ABC", "http://example/abc");
        // Updaing the system is asynchronous
        await().atMost(5, SECONDS).until(()->dLink1.exists(log2));
        
        int fusekiPort1 = Matrix.choosePort();
        int fusekiPort2 = Matrix.choosePort();

        
        // Set primary delta server.
        // Retry needed?
        Matrix.setupFuseki("ABC", "target/Zone1", fusekiPort1, deltaServerURL1, deltaServerURL2);
        Matrix.setupFuseki("ABC", "target/Zone2", fusekiPort2, deltaServerURL2, deltaServerURL1);
        
        String fusekiURL1 = "http://localhost:"+fusekiPort1+"/ABC";
        String fusekiURL2 = "http://localhost:"+fusekiPort1+"/ABC";
        
//        System.err.printf("Fuseki servers %s , %s\n", fusekiURL1, fusekiURL2);
//        System.err.printf("Delta servers %s , %s\n", deltaServerURL1, deltaServerURL2);
        
        RDFConnection conn1 = RDFConnectionFactory.connect(fusekiURL1);
        RDFConnection conn2 = RDFConnectionFactory.connect(fusekiURL2);
        
        //boolean b = conn1.queryAsk("ASK{}");
        
        conn1.update("PREFIX : <http://delta/> INSERT DATA { :s :p :o }");
        assertCount(conn1, 1);
        assertCount(conn2, 1);
        
        Matrix.deltaServer1.stop();
        
        conn2.update("PREFIX : <http://delta/> INSERT DATA { :s :p :o2 }");

        assertCount(conn1, 2);
        assertCount(conn2, 2);

    }
    
    private static void assertCount(String msg, RDFConnection conn, long expected) {
        long x = count(conn);
        assertEquals(msg, expected, x);
    }
    private static void assertCount(RDFConnection conn, long expected) {
        long x = count(conn);
        assertEquals(expected, x);
    }
    
    private static long count(RDFConnection conn) {
        try ( QueryExecution qExec = conn.query("SELECT (count(*) AS ?count) { ?s ?p ?o }") ) {
            return qExec.execSelect().next().getLiteral("count").getLong();
        }
    }
    static <X> X retry(Supplier<X> action, int numRetries) {
        X rtn = null;
        int attempt = 0 ;
        while(rtn == null && attempt < numRetries ) {
            if ( attempt > 0 ) {
                System.err.println("Retry: "+attempt);
                // Short pause to let thread switching happen.
                Lib.sleep(100);
            }
            attempt++;
            rtn = action.get();
        }
        return rtn;
    }
        
}
