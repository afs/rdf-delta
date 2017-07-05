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

import java.io.IOException ;
import java.net.BindException ;
import java.net.ServerSocket ;

import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.fuseki.FusekiException ;
//import static org.apache.jena.fuseki.FusekiLib.choosePort();
import org.apache.jena.fuseki.embedded.FusekiEmbeddedServer ;
import org.junit.BeforeClass ;
import org.seaborne.delta.server.http.PatchLogServer ;

/**
 * Base for tests for Fuseki with Delta integration
 * 
 * @see TestDeltaFusekiGood
 * @see TestDeltaFusekiBad
 */
public class BaseDeltaFuseki {
    @BeforeClass public static void setForTesting() { 
        LogCtl.setJavaLogging("src/test/resources/logging.properties");
    }
    
    protected static int F1_PORT  =    choosePort();
    protected static int F2_PORT  =    3030;
    // Needs to be fixed - it's in the Fuseki config files.
    protected static int D_PORT   =    1068;
    
    protected static String fuseki_conf1 = "testing/fuseki_conf_1.ttl"; 
    protected static String fuseki_conf2 = "testing/fuseki_conf_2.ttl";
    protected static String ds1          = "/ds1";
    protected static String ds2          = "/ds2";
    protected static String deltaServerBase = "target/DeltaServerFuseki";
    
    protected static String PREFIX = "PREFIX : <http://example/>\nPREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n";

    // Use FusekiLib.choosePort() when 2.7.0 is out.
    /** Choose an unused port for a server to listen on */
    public static int choosePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException ex) {
            throw new FusekiException("Failed to find a port");
        }
    }
    
    protected static PatchLogServer patchLogServer() {
        return patchLogServer(D_PORT, deltaServerBase);
    }
    
    protected static PatchLogServer patchLogServer(int port, String base) {
        // --- Reset state.
        FileOps.ensureDir(base);
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

    protected static FusekiEmbeddedServer fuseki1() {
        return fuseki(F1_PORT, fuseki_conf1);
    }
    
    protected static FusekiEmbeddedServer fuseki2() {
        return fuseki(F2_PORT, fuseki_conf2);
    }
    
    protected static FusekiEmbeddedServer fuseki(int port, String config) {
        return FusekiEmbeddedServer.create().setPort(port).parseConfigFile(config).build().start();
    }
    
}
