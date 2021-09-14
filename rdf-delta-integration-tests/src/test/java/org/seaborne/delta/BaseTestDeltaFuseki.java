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

import java.io.StringReader;
import java.net.BindException ;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.lib.IRILib;
import org.apache.jena.atlas.web.WebLib;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.junit.BeforeClass ;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.lib.LogX;
import org.seaborne.delta.server.http.DeltaServer;
import org.seaborne.delta.server.local.DPS;

/**
 * Base for tests for Fuseki with Delta integration
 *
 * @see TestDeltaFusekiGood
 * @see TestDeltaFusekiBad
 */
public class BaseTestDeltaFuseki {
    @BeforeClass public static void setForTesting() {
        LogX.setJavaLogging("src/test/resources/logging.properties");
    }

    protected static int F1_PORT  =    WebLib.choosePort();
    protected static int F2_PORT  =    WebLib.choosePort();
    // Port in the configuration files. Text-replaced in tests.
    protected static String D_PORT_MARKER   =    "%D_PORT%";
    protected static int D_PORT   =    WebLib.choosePort();

    protected static String fuseki_conf1 = "testing/fuseki/fuseki_conf_1.ttl";
    protected static String fuseki_conf2 = "testing/fuseki/fuseki_conf_2.ttl";
    protected static Model fuseki_conf_m1;
    protected static Model fuseki_conf_m2;
    protected static String ds1          = "/ds1";
    protected static String ds2          = "/ds2";
    protected static String deltaServerBase = "target/DeltaServerFuseki";

    protected static String PREFIX = "PREFIX : <http://example/>\nPREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n";

    protected static DeltaServer deltaServer() {
        return deltaServer(Start.CLEAN);
    }

    protected static DeltaServer deltaServer(Start state) {
        return deltaServer(state, D_PORT, deltaServerBase);
    }

    protected static DeltaServer deltaServer(Start state, int port, String base) {
        switch (state) {
            case CLEAN : {
                DPS.resetSystem();
                FileOps.ensureDir(base);
                FileOps.clearAll(base);
                break;
            }
            case RESTART :
                break;
        }
        DeltaServer server = DeltaServer.server(port, base);
        try {
            server.start();
            return server;
        } catch(BindException ex) {
            Delta.DELTA_LOG.error("Address in use: port="+port);
            return null;
        }
    }

    protected enum Start { CLEAN, RESTART };

    protected static FusekiServer fuseki1() {
        return fuseki1(Start.CLEAN);
    }

    protected static FusekiServer fuseki2() {
        return fuseki2(Start.CLEAN);
    }

    protected static FusekiServer fuseki1(Start state) {
        return fuseki(state, F1_PORT, D_PORT, fuseki_conf1, "target/Zone1");
    }

    protected static FusekiServer fuseki2(Start state) {
        return fuseki(state, F2_PORT, D_PORT, fuseki_conf2, "target/Zone2");
    }

    private static FusekiServer fuseki(Start state, int port, int deltaPort, String config, String zone) {
        // Replace %D_PORT%
        String text = IO.readWholeFileAsUTF8(config);
        String baseIRI = IRILib.filenameToIRI(config);
        text = text.replace(D_PORT_MARKER, Integer.toString(deltaPort));
        Model confModel = ModelFactory.createDefaultModel();
        RDFParser.create().base(baseIRI).source(new StringReader(text)).lang(Lang.TTL).parse(confModel);

        switch (state) {
            case CLEAN : {
                Zone.clearZoneCache();
                FileOps.clearDirectory(zone);
                break;
            }
            case RESTART :
                break;
        }
        return FusekiServer.create().port(port).loopback(true).parseConfig(confModel).build().start();
    }
}
