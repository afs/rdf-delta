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

package org.seaborne.delta ;

import static org.junit.Assert.* ;

import org.apache.http.client.HttpClient ;
import org.apache.http.impl.client.CloseableHttpClient ;
import org.apache.http.impl.client.HttpClients ;
import org.apache.jena.assembler.exceptions.AssemblerException ;
import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.fuseki.embedded.FusekiServer ;
import org.apache.jena.query.QueryExecution ;
import org.apache.jena.rdfconnection.RDFConnection ;
import org.apache.jena.rdfconnection.RDFConnectionFactory ;
import org.apache.jena.riot.web.HttpOp ;
import org.apache.jena.sparql.engine.http.QueryExceptionHTTP ;
import org.junit.* ;
import org.seaborne.delta.server.http.PatchLogServer ;

/**
 * Tests for Fuseki with Delta integration when things are not going well.
 * 
 * @see TestDeltaFusekiBad
 */
public class TestDeltaFusekiBad extends BaseDeltaFuseki {

    private static HttpClient dftStdHttpClient;
    
    @Before
    public void before() { HttpOp.setDefaultHttpClient(HttpClients.createMinimal()); }

    @After
    public void after() { 
        IO.close( ((CloseableHttpClient)HttpOp.getDefaultHttpClient()) );
    }

    @BeforeClass
    public static void cpatureHttpOp() {
        dftStdHttpClient = HttpOp.getDefaultHttpClient() ;
    }

    @AfterClass
    public static void resetHttpOp() {
        HttpOp.setDefaultHttpClient(dftStdHttpClient) ;
    }

    @Test(expected=QueryExceptionHTTP.class)
    public void fuseki_stop() {
        PatchLogServer patchLogServer = patchLogServer();
        FusekiServer server1 = fuseki1();
        try { 
            server1.stop();
            RDFConnection conn1 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+ds1) ;
            QueryExecution qExec = conn1.query("ASK{}");
            qExec.execAsk();
        } finally {
            patchLogServer.stop();
        }
    }
    
    @Test(expected=AssemblerException.class)
    public void fuseki_start() {
        // No PatchLogServer running.
        //PatchLogServer patchLogServer = patchLogServer();
        
        // AssemblerException -> HttpException -> NoHttpResponseException
        FusekiServer server1 = fuseki1();
        server1.stop();

        RDFConnection conn1 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+ds1) ;
        QueryExecution qExec = conn1.query("ASK{}");
        qExec.execAsk();
    }

    @Test
    public void fuseki_stop_start() {
        PatchLogServer patchLogServer = patchLogServer();
        FusekiServer server1 = fuseki1();
        try { 
            server1.stop();
            
            RDFConnection conn1 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+ds1) ;
            QueryExecution qExec = conn1.query("ASK{}");
            try { qExec.execAsk(); fail(); } catch(QueryExceptionHTTP ex) {} 
            // Restart, same port.
            server1 = fuseki1();
            QueryExecution qExec1 = conn1.query("ASK{}");
            qExec1.execAsk();
        } finally {
            server1.stop();
            patchLogServer.stop();
        }
    }
    
    @Test
    public void patchserver_stop_start() {
        PatchLogServer patchLogServer = patchLogServer();
        FusekiServer server1 = fuseki1();
        try { 
            // Restart.
            patchLogServer.stop();
            
            // Should fail
            try {
                RDFConnection conn0 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+ds1) ;
                conn0.update(PREFIX+"INSERT DATA { :s :p 'update_patchserver_stop_start' }");
                Assert.fail("Should not be able to update at the moment");
            } catch (HttpException ex) {
                // 503 Service Unavailable
                assertEquals(503, ex.getResponseCode());
            }
            
            patchLogServer = patchLogServer(); 

            RDFConnection conn1 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+ds1) ;
            QueryExecution qExec = conn1.query("ASK{}");
            // 500 due to offline patchLogServer.
            qExec.execAsk();
        } finally {
            server1.stop();
            patchLogServer.stop();
        }
    }
}
