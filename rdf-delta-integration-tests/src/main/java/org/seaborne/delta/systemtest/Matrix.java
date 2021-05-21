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

package org.seaborne.delta.systemtest;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jena.atlas.lib.ThreadLib.async;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.logging.FmtLog;
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
import org.seaborne.delta.server.http.DeltaServer;
import org.seaborne.delta.server.local.DPS;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.LocalServer;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.LocalServers;
import org.seaborne.delta.zk.ZkS;
import org.seaborne.delta.zk.ZooServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Two DeltaServers, one testing Zk server */
public class Matrix {

    static Logger LOG = LoggerFactory.getLogger("Matrix");

    static List<Integer> ports = new ArrayList<>();

    private static TestingServer zkServer = null;

    public static String zookeeperConnectionString = null;

    public static int deltaPort1 = -1;
    public static int deltaPort2 = -1;

    public static DeltaServer deltaServer1 = null;
    public static DeltaServer deltaServer2 = null;

    public static String deltaServerURL1 = null;
    public static String deltaServerURL2 = null;

    // The local links to the servers.
    public static DeltaLink deltaServerLink1 = null;
    public static DeltaLink deltaServerLink2 = null;

    private static final List<CuratorFramework> curatorClients = new ArrayList<>();
    private static final List<LocalServer> localServers = new ArrayList<>();

    public static void setup() {
        zookeeperConnectionString = startZooJVM();

        deltaPort1 = choosePort();
        deltaServerURL1 = "http://localhost:"+deltaPort1+"/";
        Pair<DeltaLink, DeltaServer> p1 = startDeltaServer(deltaPort1, zookeeperConnectionString);
        deltaServerLink1 = p1.getLeft() ;
        deltaServer1 = p1.getRight();

        Lib.sleep(500);
        // Race condition on formatting!
        deltaPort2 = choosePort();
        deltaServerURL2 = "http://localhost:"+deltaPort2+"/";

        Pair<DeltaLink, DeltaServer> p2 = startDeltaServer(deltaPort2, zookeeperConnectionString);
        deltaServerLink2 = p2.getLeft() ;
        deltaServer2 = p2.getRight();
    }

    public static void teardown() throws IOException {
        curatorClients.forEach(CuratorFramework::close);
        curatorClients.clear();
        localServers.forEach(LocalServer::shutdown);
        DPS.resetSystem();

        deltaServer1.stop();
        deltaServer2.stop();
        if ( zkServer != null ) {
            zkServer.stop();
            zkServer.close();
        }
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

    public static Pair<DeltaLink, DeltaServer> startDeltaServer(int port, String connectionString) {
        LocalServerConfig config = LocalServers.configZk(connectionString);
        return startDeltaServer(port, config);
    }

    public static Pair<DeltaLink, DeltaServer> startDeltaServer(int port, LocalServerConfig config) {
        LocalServer server = LocalServer.create(config);
        localServers.add(server);
        DeltaLink link = DeltaLinkLocal.connect(server);
        DeltaServer deltaServer = DeltaServer.create(port, link) ;
        try { deltaServer.start(); }
        catch(BindException ex) {
            FmtLog.error(LOG, "Address in use: port=%d", port);
        }
        return Pair.create(link,  deltaServer);
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

    public static DeltaLink setupFuseki(String dsName, String zoneDir, int fusekiPort, String...deltaServers) {
        List<DeltaLink> links = new ArrayList<>(deltaServers.length);
        for ( String destURL  : deltaServers ) {
            DeltaLink link = DeltaLinkHTTP.connect(destURL);
            links.add(link);
        }
        DeltaLink deltaLink = new DeltaLinkSwitchable(links);
        setupFuseki(fusekiPort, dsName, zoneDir, deltaLink);
        return deltaLink;
    }

    private static void setupFuseki(int fusekiPort, String dsName, String zoneDir, DeltaLink deltaLink) {
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
                .loopback(true)
                .port(fusekiPort)
                .add(dsName, dsg)
                .build();
        server.start();
    }

    private static DeltaClient setup_dataset(String dsName, String zoneDir, DeltaLink dLink) {
        // Probe to see if it exists.
        DataSourceDescription dsd = dLink.getDataSourceDescriptionByName(dsName);
        if ( dsd == null ) {
            LOG.warn("Setup Fuseki: Null DataSource for "+dsName);
        }

        // Ephemeral Zone not supported.
        Zone zone = Zone.connect(zoneDir);
        DeltaClient dClient = DeltaClient.create(zone, dLink);
        return dClient;
    }

    /**
     * Choose an unused port, not already allocated by this process. Imperfect but close
     * so try to use the port quite soon. {@code new ServerSocket(0)} does tend to return
     * different ports each time so it is rare to not be able to use the port due to
     * another process on the machine.
     */
    public static int choosePort() {
        for (var i = 0; i < 10; i++) {
            try (ServerSocket s = new ServerSocket(0)) {
                int port = s.getLocalPort();
                if (!ports.contains(port)) {
                    ports.add(port);
                    return port;
                }
            } catch (IOException ex) {
                throw new RuntimeException("Failed to find a port");
            }
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
