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

import static org.junit.Assert.assertTrue;

import java.net.BindException;

import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.atlas.web.WebLib;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.sparql.sse.SSE;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.server.http.DeltaServer;
import org.seaborne.delta.server.local.DPS;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.LocalServers;

/**
 * Other tests for Delta+Fuseki, not using the setup in {@link BaseTestDeltaFuseki}
 *
 * @see TestDeltaFusekiGood
 * @see TestDeltaFusekiBad
 */
public class TestDeltaFuseki {
    @BeforeClass public static void setForTesting() {
        LogCtl.setJavaLogging("src/test/resources/logging.properties");
    }

    @Before public void beforeClass() { DPS.resetSystem(); }
    @After public void afterClass() { DPS.resetSystem(); }

    @Test public void test1() {
        int PORT1 = WebLib.choosePort();
        int PORT2 = WebLib.choosePort();

        DeltaServer deltaServer = runServer();

        // Know port in assembler file - 1077
        FusekiServer.Builder builder = FusekiServer.create().loopback(true).parseConfigFile("testing/fuseki/fuseki-assembler-ext.ttl");


        FusekiServer fusekiServer1 = builder.port(PORT1).build();
        FusekiServer fusekiServer2 = builder.port(PORT2).build();

        fusekiServer1.start();
        fusekiServer2.start();

        Triple t = SSE.parseTriple("(<x:s> <x:p> <x:o>)");

        RDFConnection conn1 = RDFConnectionFactory.connect("http://localhost:"+PORT1+"/ds");
        try ( RDFConnection conn1a = conn1 ) {
            conn1.update("INSERT DATA { <x:s> <x:p> <x:o> }");
            boolean b = conn1.queryAsk("ASK { <x:s> <x:p> <x:o> }");
            assertTrue(b);
        }

        RDFConnection conn2 = RDFConnectionFactory.connect("http://localhost:"+PORT2+"/ds");
        try ( RDFConnection conn2a = conn2 ) {
            boolean b = conn2.queryAsk("ASK { <x:s> <x:p> <x:o> }");
            assertTrue(b);
        }

        deltaServer.stop();
    }

    private static DeltaServer runServer() {
        LocalServerConfig localServerConfig = LocalServers.configMem();
        DeltaServer server = DeltaServer.create(1077, localServerConfig);
        try {
            // Server.
            server.start();
            return server;
        } catch ( BindException ex) {
            throw IOX.exception(ex);
        }
    }

}
