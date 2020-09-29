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

package org.seaborne.delta ;

import static org.apache.jena.atlas.lib.ThreadLib.async;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.seaborne.delta.BaseTestDeltaFuseki.Start.CLEAN;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.http.client.HttpClient ;
import org.apache.http.impl.client.CloseableHttpClient ;
import org.apache.http.impl.client.HttpClients ;
import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.QueryExecution ;
import org.apache.jena.rdfconnection.RDFConnection ;
import org.apache.jena.rdfconnection.RDFConnectionFactory ;
import org.apache.jena.riot.web.HttpOp ;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.seaborne.delta.server.http.DeltaServer;

/**
 * Tests for Fuseki with Delta integration when things are going well.
 * Based on config files so the port number is fixed.
 *
 * @see TestDeltaFusekiBad
 */
public class TestDeltaFusekiGood extends BaseTestDeltaFuseki {
    // May leak a few TIME_WAIT connections.
    protected static FusekiServer server1;
    protected static FusekiServer server2;
    protected static DeltaServer  deltaServer;
    protected static RDFConnection conn1;
    protected static RDFConnection conn2;
    protected static String URL_DPS;
    private static HttpClient dftStdHttpClient = null;

    @BeforeClass
    public static void beforeClass() {
        try {

            dftStdHttpClient = HttpOp.getDefaultHttpClient();

            HttpOp.setDefaultHttpClient(HttpClients.createMinimal());

            deltaServer = deltaServer(CLEAN);

            server1 = fuseki1(CLEAN);
            server2 = fuseki2(CLEAN);

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
        deltaServer.stop();
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

    @Test
    public void concurrent_update() {
        String slowUpdate = "PREFIX afn: <http://jena.apache.org/ARQ/function#> INSERT { <x:s> <x:p> 'abc' } WHERE { FILTER(afn:wait(2000)) }";
        String smallUpdate = "DELETE { <x:s> <x:p> ?X } INSERT { <x:s> <x:p> 'def' } WHERE {  <x:s> <x:p> ?X  }; INSERT DATA { <x:s> <x:p> 'xyz' }" ;

        AtomicReference<Throwable> asyncOutcome = new AtomicReference<>();
        Semaphore sema = new Semaphore(0);

        // Slow.
        Runnable slow = ()->{
            try {
                conn1.update(slowUpdate);
            } catch (HttpException ex) {
                asyncOutcome.set(ex);
            }
            sema.release();
        };

//            conn2.queryResultSet("SELECT * { ?s ?p ?o}" , ResultSetFormatter::out);

        async(slow);
        // Let the async go first. Then we get to send to Fuseki which blocks.
        // This isn't great to make sure the slow update is now in the wait (lock held) point
        // but (1) if it isn't the test passes (20 it gets run in many environments so some will fail sometime.
        Lib.sleep(250);
        conn2.update(smallUpdate);

        try {
            sema.tryAcquire(10,  TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("Failed to get semaphore");
        }
        if (asyncOutcome.get() != null )
            asyncOutcome.get().printStackTrace();
        assertNull(asyncOutcome.get());
    }
}
