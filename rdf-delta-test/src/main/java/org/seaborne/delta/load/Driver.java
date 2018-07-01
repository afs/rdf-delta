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

package org.seaborne.delta.load;

import java.net.BindException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.http.client.HttpClient;
import org.apache.jena.atlas.lib.DateTimeUtils;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.FusekiLib;
import org.apache.jena.fuseki.build.FusekiConfig;
import org.apache.jena.fuseki.embedded.FusekiServer;
import org.apache.jena.fuseki.embedded.FusekiServer.Builder;
import org.apache.jena.fuseki.server.DataAccessPoint;
import org.apache.jena.fuseki.server.FusekiVocab;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sparql.util.graph.GraphUtils;
import org.apache.jena.update.UpdateAction;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.server.http.PatchLogServer;

/** Drive updates and reads */


public class Driver {
    protected static final Path DIR = Paths.get("target/loadtest");
    static { 
        FileOps.ensureDir("target");
        FileOps.ensureDir(DIR.toString());
    }
    
    
    protected static int DELTA_PORT = FusekiLib.choosePort();
    protected static String DELTA_DIR = DIR.resolve("DeltaBase").toString();
    protected static String PATCH_LOG_NAME = "ABC";
    
    protected static int F1_PORT = FusekiLib.choosePort();
    protected static int F2_PORT = FusekiLib.choosePort();
    protected static String DS_NAME = "/DS";
    protected static Path ZONE1 = DIR.resolve("Zone1");
    protected static Path ZONE2 = DIR.resolve("Zone2");
    
    protected static String DS1_NAME = "/DS1";
    protected static String DS2_NAME = "/DS2";
    
    protected static FusekiServer server1;
    protected static FusekiServer server2;
    protected static PatchLogServer  patchLogServer;
    private static HttpClient dftStdHttpClient = null;

    static { LogCtl.setJavaLogging(); }
    
    public static void main(String[] args) throws InterruptedException {
        // Fix up fuseki config files.
        // DELTA_PORT => value.

        // This is a template that needs updating,
        // Server 1.
        
        Model model = RDFDataMgr.loadModel("fuseki-config.ttl"); 
        //  fuseki:name    "%DS_NAME%"
        //  delta:changes  "%LOG_URL%"
        //  delta:patchlog "%LOG_NAME%"
        //  delta:zone     "%ZONE_NAME%"
        update(model, "%DS_NAME%", DS_NAME);
        String LOG_URL = "http://localhost:"+DELTA_PORT+"/";
        update(model, "%LOG_URL%", LOG_URL);
        update(model, "%LOG_NAME%", PATCH_LOG_NAME);
        
        String zone1 = ZONE1.toString();
        String zone2 = ZONE2.toString();
        
        update(model, "%ZONE_NAME%", zone1);
        
        // --- Reset state.
        if ( true ) {
            FileOps.ensureDir(DELTA_DIR);
            FileOps.clearAll(DELTA_DIR);
            FileOps.ensureDir(zone1);
            FileOps.clearAll(zone1);
            FileOps.ensureDir(zone2);
            FileOps.clearAll(zone2);
        }
        
        PatchLogServer logServer = patchLogServer(DELTA_PORT, DELTA_DIR);
        
        try {
            logServer.start();
        }
        catch (BindException e) {
            e.printStackTrace();
            System.exit(0);
        }

        
//        RDFDataMgr.write(System.out,  model, Lang.TTL);
//        System.out.flush();
        FusekiServer server1 = fuseki(F1_PORT, model);
        server1.start();
        //FusekiServer server2 = fuseki2();
        
        int numClients = 10;
        int clientLoops = 10;
        
        CountDownLatch cdl1 = new CountDownLatch(numClients);
        CountDownLatch cdl2 = new CountDownLatch(numClients);
        for (int i = 0 ; i < numClients ; i++ ) {
            client(clientLoops, cdl1, cdl2);
        }
        cdl2.await();
        logServer.stop();
        System.out.println("DONE");
        System.exit(0);
    }
    
    static void client(int loops, CountDownLatch cdlStart, CountDownLatch cdlFinish) {
        //conn2 = RDFConnectionFactory.connect("http://localhost:"+F2_PORT+DS_NAME) ;
        
        Runnable r = ()->{
            RDFConnection conn1 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+DS_NAME) ;
            try {
                cdlStart.countDown();
                cdlStart.await();
            }
            catch (InterruptedException e) {
                e.printStackTrace(System.err);
                return ;
            }
            
            try (RDFConnection conn = conn1) {
                for ( int i = 0 ; i < loops ; i++ ) {
                    String now = DateTimeUtils.nowAsXSDDateTimeString()+"-"+i;
                    try {
                        // This can abort and the update is then lost 
                        conn.update("INSERT DATA { <x:s> <x:p> '"+now+"' }");
                        
                    } catch (DeltaException ex) {
                        System.out.flush();
                        System.err.println("\nSystem abort\n");
                    }
                    try ( QueryExecution qExec = conn.query("SELECT * { ?s ?p ?o}") ) {
                        ResultSetFormatter.consume(qExec.execSelect());
                        // QueryExecUtils.executeQuery(qExec);
                    }

                }
            }
            cdlFinish.countDown();
        };
        
        new Thread(r).start();
    }
    
    
    
    static void update(Model model, String param, String value) {
        String us = "DELETE { ?s ?p '%1%' } INSERT { ?s ?p '%2%' } WHERE { ?s ?p '%1%' }";
        us = us.replace("%1%", param);
        us = us.replace("%2%", value);
        //System.out.println(us);
        UpdateAction.parseExecute(us, model);
    }
    
    protected static FusekiServer fuseki(int port, Model assembler) {
        Builder builder = 
            FusekiServer.create()
                .setPort(port);
      // Process server context
      Resource server = GraphUtils.getResourceByType(assembler, FusekiVocab.tServer);
      if ( server != null )
          AssemblerUtils.setContext(server, Fuseki.getContext()) ;
      // Process services, whether via server ja:services or, if absent, by finding by type. 
      List<DataAccessPoint> x = FusekiConfig.servicesAndDatasets(assembler);
      // Unbundle so that they accumulate.
      x.forEach(dap->builder.add(dap.getName(), dap.getDataService()));
      return builder.build();
    }

//        // From FusekiServer.Builder.
//    /** Read and parse a Fuseki services/datasets file.
//     *  <p>
//     *  The application is responsible for ensuring a correct classpath. For example,
//     *  including a dependency on {@code jena-text} if the configuration file
//     *  includes a text index.     
//     */
//    public Builder parseConfigFile(String filename) {
//        requireNonNull(filename, "filename");
//        Model model = AssemblerUtils.readAssemblerFile(filename);
//        
//        // Fix up model.
//        UpdateAction
//        
//        
//        // Process server context
//        Resource server = GraphUtils.getResourceByType(model, FusekiVocab.tServer);
//        if ( server != null )
//            AssemblerUtils.setContext(server, Fuseki.getContext()) ;
//
//        // Process services, whether via server ja:services or, if absent, by finding by type. 
//        List<DataAccessPoint> x = FusekiConfig.servicesAndDatasets(model);
//        // Unbundle so that they accumulate.
//        x.forEach(dap->add(dap.getName(), dap.getDataService()));
//    }

    
    protected static PatchLogServer patchLogServer(int port, String base) {
        PatchLogServer dps = PatchLogServer.server(port, base);
        try { 
            dps.start();
            return dps; 
        } catch(BindException ex) {
            Delta.DELTA_LOG.error("Address in use: port="+port);
            return null;
        }
    }



    
//    @BeforeClass
//    public static void beforeClass() {
//        dftStdHttpClient = HttpOp.getDefaultHttpClient();
//        
//        HttpOp.setDefaultHttpClient(HttpClients.createMinimal());
//        
//        patchLogServer = patchLogServer();
//        server1 = fuseki1();
//        server2 = fuseki2();
//        
//        URL_DPS = "http://localhost:"+D_PORT+"/";
//        
//        conn1 = RDFConnectionFactory.connect("http://localhost:"+F1_PORT+ds1) ;
//        conn2 = RDFConnectionFactory.connect("http://localhost:"+F2_PORT+ds2) ;
//    }
//    
//    @AfterClass
//    public static void afterClass() {
//        server1.stop();
//        server2.stop();
//        patchLogServer.stop();
//        IO.close( ((CloseableHttpClient)HttpOp.getDefaultHttpClient()) );
//        HttpOp.setDefaultHttpClient(dftStdHttpClient);
//    }
//
//    @Before public void before() {}
//    @After  public void after()  {}
//
//    @Test public void basic_1() {
//        conn1.query("ASK{}");
//        conn2.query("ASK{}");
//    }
//    
//    @Test public void update_1() {
//        // Do an update on one server ...
//        conn1.update(PREFIX+"INSERT DATA { :s :p 'update_1' }");
//        // And see it on the other server ...
//        try ( QueryExecution qExec = conn2.query(PREFIX+"ASK { :s :p 'update_1'}") ) {
//            Assert.assertEquals(true, qExec.execAsk());
//        }
//    }
//    
//    @Test public void update_2() {
//        // Do an update on one server ...
//        conn1.update(PREFIX+"INSERT DATA { :s :p 'update_2_A' }");
//        // And on the other server ...
//        conn2.update(PREFIX+"INSERT DATA { :s :p 'update_2_B' }");
//        
//        // Ask each server ...
//        try ( QueryExecution qExec = conn2.query(PREFIX+"ASK { :s :p 'update_2_A', 'update_2_B' }") ) {
//            Assert.assertEquals(true, qExec.execAsk());
//        }
//        try ( QueryExecution qExec = conn1.query(PREFIX+"ASK { :s :p 'update_2_A', 'update_2_B' }") ) {
//            Assert.assertEquals(true, qExec.execAsk());
//        }
//    }

}
