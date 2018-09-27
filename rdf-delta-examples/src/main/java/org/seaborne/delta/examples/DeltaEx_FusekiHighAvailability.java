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

package org.seaborne.delta.examples;

import java.io.IOException ;
import java.net.BindException ;
import java.net.ServerSocket ;

import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.fuseki.FusekiException ;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.rdf.model.Model ;
import org.apache.jena.rdfconnection.RDFConnection ;
import org.apache.jena.rdfconnection.RDFConnectionFactory ;
import org.apache.jena.riot.Lang ;
import org.apache.jena.riot.RDFDataMgr ;
import org.seaborne.delta.server.http.PatchLogServer ;

/**
 * Create two Fuseki servers, each with a dataset. These dataset are kept in step as
 * updates happen at either server because they share a patch log run by a backend server.
 * <p>
 * Combined with a load balancer in front of the two (or more) Fuseki servers, this gives
 * high availability of the dataset, robust against loss of some of the Fuseki servers.
 * Such loss maybe panned (e.g server adminstration) or unplanned (e.g. server crash).
 * <p>
 * When a new server is addedd, it automatically catches up with the current state of the
 * dataset.
 * <p>
 * For convenience of this example, the datasets and the the patch log server are in the
 * same JVM; no state is shared and all interaction is over HTTP. This is not a realistic
 * deploment; each Fuseki server and patch log server should be running on separate
 * machines.
 */

public class DeltaEx_FusekiHighAvailability {
    static { LogCtl.setJavaLogging(); }

    final static int    F1_PORT       = choosePort() ;
    final static String FUSEKI_CONF_1 = "ExampleFusekiConfigs/fuseki_conf_1.ttl" ;
    final static String ZONE1         = "Zone1" ;

    final static int    F2_PORT       = choosePort() ;
    final static String FUSEKI_CONF_2 = "ExampleFusekiConfigs/fuseki_conf_2.ttl" ;
    final static String ZONE2         = "Zone2" ;

    // This is know by the configuration files.
    final static int PLOG_PORT = 1069;
    final static String PLOG_DIR = "DeltaServer";

    final static String DS_NAME = "ABC";

    public static void main(String ...args) {
        try { main2(args) ; }
        finally { System.exit(0); }
    }

    public static void main2(String ...args) {
        setup();
        // Patch Log Server
        FileOps.exists(PLOG_DIR);
        FileOps.clearAll(PLOG_DIR);

        PatchLogServer patchLogServer = PatchLogServer.server(PLOG_PORT, PLOG_DIR);
        try { patchLogServer.start(); }
        catch (BindException ex) {
            System.err.println("Can't start the patch log server: "+ex.getMessage());
            System.exit(1);
        }

        // For high availability, need a load balancer that switches between the two Fuskei servers.

        // Fuseki server 1
        FusekiServer fuseki1 = fuseki1();
        RDFConnection conn1 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+"/ds1") ;

        // Fuseki server 2
        FusekiServer fuseki2 = fuseki2();
        RDFConnection conn2 = RDFConnectionFactory.connect("http://localhost:"+F2_PORT+"/ds2") ;

        // Some data (data.ttl is in src/main/resources).
        Model model = RDFDataMgr.loadModel("data.ttl");
        conn1.put(model);

        // Kill fuseki1.
        fuseki1.stop();

        // And fetch data.
        Model model2 = conn2.fetch();
        System.out.println();
        RDFDataMgr.write(System.out, model2, Lang.NT);
        System.out.println();

        // Remove a triple via conn2.
        conn2.update("PREFIX ex: <http://example.org/> DELETE DATA { ex:s ex:p ex:o }");

        // Restart Fuseki1.
        fuseki1 = fuseki1();
        // Not necesary.
        // conn1 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+"/ds1") ;
        Model model1 = conn1.fetch();
        System.out.println();
        // Data in Fuseki1. One less triple.
        RDFDataMgr.write(System.out, model1, Lang.NT);
        System.out.println();
    }


    protected static FusekiServer fuseki1() {
        return fuseki(F1_PORT, FUSEKI_CONF_1);
    }

    protected static FusekiServer fuseki2() {
        return fuseki(F2_PORT, FUSEKI_CONF_2);
    }

    protected static FusekiServer fuseki(int port, String config) {
        return FusekiServer.create().port(port).parseConfigFile(config).build().start();
    }

    public static void setup() {
        FileOps.exists(PLOG_DIR);
        FileOps.clearAll(PLOG_DIR);
        FileOps.exists(ZONE1);
        FileOps.clearAll(ZONE1);
        FileOps.exists(ZONE2);
        FileOps.clearAll(ZONE2);
    }



    public static int choosePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException ex) {
            throw new FusekiException("Failed to find a port");
        }
    }

}
