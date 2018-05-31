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

import org.apache.http.client.HttpClient ;
import org.apache.http.impl.client.CloseableHttpClient ;
import org.apache.http.impl.client.HttpClients ;
import org.apache.jena.atlas.io.IO ;
import org.apache.jena.fuseki.embedded.FusekiServer ;
import org.apache.jena.query.QueryExecution ;
import org.apache.jena.rdfconnection.RDFConnection ;
import org.apache.jena.rdfconnection.RDFConnectionFactory ;
import org.apache.jena.riot.web.HttpOp ;
import org.junit.* ;
import org.seaborne.delta.server.http.PatchLogServer ;
import static org.seaborne.delta.BaseTestDeltaFuseki.Start.*;

/**
 * Tests for Fuseki with Delta integration when things are going well.
 * 
 * @see TestDeltaFusekiBad
 */
public class TestDeltaFusekiGood extends BaseTestDeltaFuseki {
    // May leak a few TIME_WAIT connections.
    protected static FusekiServer server1;
    protected static FusekiServer server2;
    protected static PatchLogServer  patchLogServer;
    protected static RDFConnection conn1; 
    protected static RDFConnection conn2;
    protected static String URL_DPS;
    private static HttpClient dftStdHttpClient = null;
    
    @BeforeClass
    public static void beforeClass() {
        try {
        
        dftStdHttpClient = HttpOp.getDefaultHttpClient();
        
        HttpOp.setDefaultHttpClient(HttpClients.createMinimal());
        
        patchLogServer = patchLogServer(CLEAN);
        
        // This needs testing.
        server1 = fuseki1(CLEAN);
        server2 = fuseki2(CLEAN); // Can not create!
        
        URL_DPS = "http://localhost:"+D_PORT+"/";
        
        conn1 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+ds1) ;
        conn2 = RDFConnectionFactory.connect("http://localhost:"+F2_PORT+ds2) ;
        
        } catch (Throwable th) {
            th.printStackTrace();
        }
    }
    
    @AfterClass
    public static void afterClass() {
        if ( server1 != null )
            server1.stop();
        if ( server2 != null )
            server2.stop();
        patchLogServer.stop();
        IO.close( ((CloseableHttpClient)HttpOp.getDefaultHttpClient()) );
        HttpOp.setDefaultHttpClient(dftStdHttpClient);
    }

    @Before public void before() {}
    @After  public void after()  {}

    @Test 
    //@Ignore
    public void basic_1() {
        conn1.query("ASK{}");
        conn2.query("ASK{}");
    }
    
    @Test
    public void update_1() {
        // Do an update on one server ...
        conn1.update(PREFIX+"INSERT DATA { :s :p 'update_1' }");
        // And see it on the other server ...
        try ( QueryExecution qExec = conn2.query(PREFIX+"ASK { :s :p 'update_1'}") ) {
            Assert.assertEquals(true, qExec.execAsk());
        }
    }
    
    @Test
    public void update_2() {
        // Do an update on one server ...
        conn1.update(PREFIX+"INSERT DATA { :s :p 'update_2_A' }");
        // And on the other server ...
        conn2.update(PREFIX+"INSERT DATA { :s :p 'update_2_B' }");
        
        // Ask each server ...
        try ( QueryExecution qExec = conn2.query(PREFIX+"ASK { :s :p 'update_2_A', 'update_2_B' }") ) {
            Assert.assertEquals(true, qExec.execAsk());
        }
        try ( QueryExecution qExec = conn1.query(PREFIX+"ASK { :s :p 'update_2_A', 'update_2_B' }") ) {
            Assert.assertEquals(true, qExec.execAsk());
        }
    }
}
