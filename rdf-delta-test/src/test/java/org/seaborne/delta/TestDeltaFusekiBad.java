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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.seaborne.delta.BaseTestDeltaFuseki.Start.CLEAN;

import org.apache.http.client.HttpClient ;
import org.apache.http.impl.client.CloseableHttpClient ;
import org.apache.http.impl.client.HttpClients ;
import org.apache.jena.assembler.exceptions.AssemblerException ;
import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.QueryExecution ;
import org.apache.jena.rdfconnection.RDFConnection ;
import org.apache.jena.rdfconnection.RDFConnectionFactory ;
import org.apache.jena.riot.web.HttpOp ;
import org.apache.jena.sparql.engine.http.QueryExceptionHTTP ;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.seaborne.delta.server.http.DeltaServer ;

/**
 * Tests for Fuseki with Delta integration when things are not going well.
 *
 * @see TestDeltaFusekiBad
 */
public class TestDeltaFusekiBad extends BaseTestDeltaFuseki {

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
        DeltaServer deltaServer = deltaServer(CLEAN);
        FusekiServer server1 = fuseki1(CLEAN);
        try {
            server1.stop();
            RDFConnection conn1 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+ds1) ;
            QueryExecution qExec = conn1.query("ASK{}");
            qExec.execAsk();
        } finally {
            deltaServer.stop();
        }
    }

    @Test(expected=AssemblerException.class)
    public void fuseki_start() {
        // No DeltaServer running.
        //DeltaServer deltaServer = deltaServer();

        // AssemblerException -> HttpException -> NoHttpResponseException

        // Assembler exception only if the dataset does not exis in the Zone.
        FusekiServer server1 = fuseki1(CLEAN);
        server1.stop();
    }

    @Test
    public void fuseki_stop_start() {
        DeltaServer deltaServer = deltaServer();
        FusekiServer server1 = fuseki1();
        try {
            server1.stop();

            RDFConnection conn1 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+ds1) ;
            QueryExecution qExec = conn1.query("ASK{}");
            try { qExec.execAsk(); fail(); } catch(QueryExceptionHTTP ex) {}
            // Restart, same port.
            server1 = fuseki1(Start.RESTART);
            QueryExecution qExec1 = conn1.query("ASK{}");
            qExec1.execAsk();
        } finally {
            server1.stop();
            deltaServer.stop();
        }
    }

    @Test
    public void patchserver_stop_start() {
        DeltaServer deltaServer = deltaServer();
        FusekiServer server1 = fuseki1();
        try {
            deltaServer.stop();
            deltaServer = null;

            // Should fail
            try (RDFConnection conn0 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+ds1) ) {
                conn0.update(PREFIX+"INSERT DATA { :s :p 'update_patchserver_stop_start' }");
                Assert.fail("Should not be able to update at the moment");
            } catch (HttpException ex) {
                assertEquals(503, ex.getResponseCode());
                // Expected - ignore.
                //assertTrue(ex.getResponseCode()>= 500);
            }

            deltaServer = deltaServer(Start.RESTART);

            try (RDFConnection conn1 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+ds1)) {
                conn1.query("ASK{}").execAsk();
            }

            // Should be able to update.
            try (RDFConnection conn0 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+ds1) ) {
                conn0.update(PREFIX+"INSERT DATA { :s :p 'update_patchserver_stop_start' }");
            }
        } finally {
            server1.stop();
            if ( deltaServer != null )
                deltaServer.stop();
        }
    }
}
