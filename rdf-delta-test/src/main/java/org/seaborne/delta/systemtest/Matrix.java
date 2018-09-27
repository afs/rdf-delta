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

package org.seaborne.delta.systemtest;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.dboe.migrate.L;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.awaitility.Awaitility;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.delta.client.DeltaClient;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.client.DeltaLinkSwitchable;
import org.seaborne.delta.client.LocalStorageType;
import org.seaborne.delta.client.SyncPolicy;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.PatchLogServer;
import org.seaborne.delta.server.local.DPS;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.LocalServer;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.LocalServers;
import org.seaborne.delta.zk.Zk;
import org.seaborne.delta.zk.ZkS;
import org.seaborne.delta.zk.ZooServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Matrix {

    static Logger LOG = LoggerFactory.getLogger("Matrix");

    static List<Integer> ports = new ArrayList<>();

    private static TestingServer zkServer = null;

    public static String zookeeperConnectionString = null;

    public static int deltaPort1 = -1;
    public static int deltaPort2 = -1;

    public static PatchLogServer deltaServer1 = null;
    public static PatchLogServer deltaServer2 = null;

    public static String deltaServerURL1 = null;
    public static String deltaServerURL2 = null;

    // The local links to the severs.
    public static DeltaLink deltaServerLink1 = null;
    public static DeltaLink deltaServerLink2 = null;

    private static List<CuratorFramework> curatorClients = new ArrayList<>();
    private static List<LocalServer> localServers = new ArrayList<>();

    public static void setup() {
        zookeeperConnectionString = startZooJVM();
//      // In-process, persistent, full server/standalone.
//      String connectionString = startZoo("ZkData");
//      // External zookeeper.
//      String connectionString = "localhost:2181,localhost:2182,localhost:2183";

      // In process 3-server ensemble.
//      String connectionString = startZooQ();

      // Start Delta servers
      // Replace with a start-stop helper.

        deltaPort1 = choosePort();
        deltaServerURL1 = "http://localhost:"+deltaPort1+"/";
        Pair<DeltaLink, PatchLogServer> p1 = startDeltaServer(deltaPort1, zookeeperConnectionString);
        deltaServerLink1 = p1.getLeft() ;
        deltaServer1 = p1.getRight();

        Lib.sleep(500);
        // Race condition on formatting!
        deltaPort2 = choosePort();
        deltaServerURL2 = "http://localhost:"+deltaPort2+"/";

        Pair<DeltaLink, PatchLogServer> p2 = startDeltaServer(deltaPort2, zookeeperConnectionString);
        deltaServerLink2 = p2.getLeft() ;
        deltaServer2 = p2.getRight();
    }

    public static void teardown() {
        curatorClients.forEach(c->c.close());
        curatorClients.clear();
        localServers.forEach(srv->srv.shutdown());
        DPS.resetSystem();

        if ( deltaServer1 != null )
            deltaServer1.stop();
        if ( deltaServer2 != null )
            deltaServer2.stop();
        if ( zkServer != null )
            try { zkServer.close(); } catch (IOException ex) { }
        deltaServer1 = null;
        deltaServer2 = null;
        deltaPort1 = -1;
        deltaPort2 = -1;
        deltaServerURL1 = null;
        deltaServerURL2 = null;
        deltaServerLink1 = null;
        deltaServerLink2 = null;
        zkServer = null;
    }

    public static void zk(Consumer<CuratorFramework> action) {
        CuratorFramework client = Zk.curator(zookeeperConnectionString);
        curatorClients.add(client);
        Zk.zkRun(()->action.accept(client));
    }

    public static Pair<DeltaLink, PatchLogServer> startDeltaServer(int port, String connectionString) {
        LocalServerConfig config = LocalServers.configZk(connectionString);
        return startDeltaServer(port, config);
        //L.async(()->DeltaServer.main("--port="+port, "--zk="+connectionString));
    }

    public static Pair<DeltaLink, PatchLogServer> startDeltaServer(int port, LocalServerConfig config) {
        LocalServer server = LocalServer.create(config);
        localServers.add(server);
        DeltaLink link = DeltaLinkLocal.connect(server);
        PatchLogServer dps = PatchLogServer.create(port, link) ;
        try { dps.start(); }
        catch(BindException ex) {
            FmtLog.error(LOG, "Address in use: port=%d", port);
        }
        //dps.join();
        return Pair.create(link,  dps);
    }

    private static String startZoo(String dataDir) {
        ZkS.zkSystemProps();
        int zkPort1 = choosePort();
        ZooServer zk1 = ZkS.runZookeeperServer(zkPort1, dataDir);
        zk1.start();
        return "localhost:"+zkPort1;
    }

    public static String startZooJVM() {
        try {
            zkServer = new TestingServer();
            zkServer.start();
            return "localhost:"+zkServer.getPort();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    // Start a whole ensemble.
    // Really messy logging.
    public static String startZooQ(boolean clean) {
        ZkS.zkSystemProps();
        String connectionString = format("localhost:2181,localhost:2182,localhost:2183");
        String [] args1 = {"./../zk/zk1/zoo.cfg"};
        String [] args2 = {"./../zk/zk2/zoo.cfg"};
        String [] args3 = {"./../zk/zk3/zoo.cfg"};

        // Port 2180
        //ZkS.runZookeeperServer("./../zk/single/zoo.cfg");

        //System.out.println("Server1 ...");
        L.async(()->QuorumPeerMain.main(args1));
        //System.out.println("Server2 ...");
        L.async(()->QuorumPeerMain.main(args2));
        //System.out.println("Server3 ...");
        L.async(()->QuorumPeerMain.main(args3));
        return connectionString;
    }

    /** One external zoo keeper */
    public static String startZooQ_single(boolean clean) {
        ZkS.zkSystemProps();
        String connectionString = format("localhost:2180");
        if ( clean )
            FileOps.clearDirectory("./../zk/single/zk-data/version-2");
        String [] args1 = {"./../zk/single/zoo.cfg"};
        L.async(()->QuorumPeerMain.main(args1));
        return connectionString;
    }

    public static DeltaLink setupFuseki(String dsName, String zoneDir, int fusekiPort, String...deltaServers) {
        if ( deltaServers.length == 0 ) {
            System.err.println("setupFuseki: no deltaServers");
            System.exit(1);
        }

        List<DeltaLink> links = new ArrayList<>(deltaServers.length);
        for ( String destURL  : deltaServers ) {
            //System.out.printf("Fuseki server: port=%d, link=%s\n", fusekiPort, destURL);
            DeltaLink link = DeltaLinkHTTP.connect(destURL);
            links.add(link);
        }
        DeltaLink deltaLink = new DeltaLinkSwitchable(links);
        setupFuseki(fusekiPort, dsName, zoneDir, deltaLink);
        return deltaLink;
    }

    private static void setupFuseki(int fusekiPort, String dsName, String zoneDir, DeltaLink deltaLink) {
        //deltaLink.register(Id.create());
        FileOps.ensureDir(zoneDir);
        FileOps.clearAll(zoneDir);

        DeltaClient dClient = setup_dataset(dsName, zoneDir, deltaLink);
        Zone zone = dClient.getZone();
        DataSourceDescription dsd = deltaLink.getDataSourceDescriptionByName(dsName);
        Id dsRef = dsd.getId();
        SyncPolicy syncPolicy = SyncPolicy.TXN_RW;


        LocalStorageType storage = LocalStorageType.MEM;
        dClient.register(dsRef, storage, syncPolicy);
        DeltaConnection deltaConnection = dClient.getLocal(dsRef);
        DatasetGraph dsg = deltaConnection.getDatasetGraph();

        FusekiServer server =
            FusekiServer.create()
                .port(fusekiPort)
                .add(dsName, dsg)
                .build();
        server.start();
    }

    // From ex7.
    private static DeltaClient setup_dataset(String dsName, String zoneDir, String patchLogServerURL) {
        DeltaLink dLink = DeltaLinkHTTP.connect(patchLogServerURL);
        return setup_dataset(dsName, zoneDir, dLink);
    }

    private static DeltaClient setup_dataset(String dsName, String zoneDir, DeltaLink dLink) {
        // Probe to see if it exists.
        DataSourceDescription dsd = dLink.getDataSourceDescriptionByName(dsName);
        if ( dsd == null ) {
            LOG.warn("Setup Fuseki: Null DataSource for "+dsName);
        }

        // Ephemeral Zone not supported.
        Zone zone = Zone.connect(zoneDir);
//        DataState dataState = zone.create(dsd.getId(), "", null, LocalStorageType.MEM);
        DeltaClient dClient = DeltaClient.create(zone, dLink);
        return dClient;

//        FileOps.exists(zoneDir);
//        FileOps.clearAll(zoneDir);
//        Zone zone = Zone.connect(zoneDir);
//        DeltaClient dClient = DeltaClient.create(zone, dLink);
//
//        // Get the Id.
//        Id dsRef;
//        if ( dsd == null )
//            dsRef = dClient.newDataSource(DS_NAME, "http://example/"+DS_NAME);
//        else
//            dsRef = dsd.getId();
//        // Create and setup locally.
//        dClient.register(dsRef, LocalStorageType.TDB, SyncPolicy.TXN_RW);
//        return dClient;
    }

    private static void cleanDirectory(String dir) {
        FileOps.ensureDir(dir);
        FileOps.clearAll(dir);
    }

    /**
     * Choose an unused port, not already allocated by this process. Imperfect but close
     * so try to use the port quite soon. {@code new ServerSocket(0)} does tend to return
     * different ports each time so it is rare to not be able to use th eport due to
     * another process on the machine.
     */
    public static int choosePort() {
        // FusekiLib.choosePort();
        try (ServerSocket s = new ServerSocket(0)) {
            int port = s.getLocalPort();
            if ( ! ports.contains(port) ) {
                ports.add(port);
                return port;
            }
        } catch (IOException ex) {
            throw new RuntimeException("Failed to find a port");
        }
        throw new RuntimeException("Failed to find a fresh port");
    }

    /** General "wait for propagation" */
    public static void await(Callable<Boolean> action) {
        Awaitility.await().pollInterval(250,  MILLISECONDS).atMost(5,  SECONDS).until(action);
    }

    public static <X> X retry(Supplier<X> action, int numRetries) {
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
