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

package org.seaborne.delta.zk;

import static org.apache.jena.atlas.lib.ThreadLib.async;

import java.io.IOException;

import org.apache.jena.atlas.RuntimeIOException;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.util.JvmPauseMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper server for running a <b>standalone</b> zookeeper server asynchronously.
 * @implNote
 * Based on ZooKeeperServerMain which runs the server synchronously.
 */
public class ZooServer {
    private static final Logger LOG = LoggerFactory.getLogger(ZooServer.class);
    // ZooKeeper server supports two kinds of connection: unencrypted and encrypted.
    private ServerCnxnFactory cnxnFactory;
    private ServerCnxnFactory secureCnxnFactory;
    private ContainerManager containerManager;

    private ZooKeeperServer zkServer;
    private FileTxnSnapLog txnLog;
    private AdminServer adminServer;

    private ServerConfig config;

    // See ZkS
//    /** @deprecated Use {@link ZkS#runZookeeperServer(String)} */
//    @Deprecated
    public static void quorumServer(String confFile) {
        // No join.
        async(() -> QuorumPeerMain.main(new String[] {confFile}) );
    }

    public ZooServer(ServerConfig config) {
        this.config = config;
    }

    /* Information: To run QuorumPeerMain
     *
     */

    /* Information: To run ZooKeeperServerMain:
      // Usage: ZooKeeperServerMain configfile | port datadir [ticktime] [maxcnxns]
      String[] a = {Integer.toString(port), dataDir};
      // Alternative - just fork it.
      L.async(() -> {
          ServerConfig config = new ServerConfig();
          config.parse(a);
          ZooKeeperServerMain zksm = new ZooKeeperServerMain();
          try {
              zksm.runFromConfig(config);
          }
          catch (IOException | AdminServerException e) {
              e.printStackTrace();
          }
      });
      // Need to wait for the server to start.
      // Better (?) : use ZooKeeperServer
      Lib.sleep(500);
    */

    public void setupFromConfig() {
        try {
            txnLog = new FileTxnSnapLog(config.getDataLogDir(), config.getDataDir());
        }
        catch (IOException e) {
            e.printStackTrace();
            IO.exception(e);
        }
        ZKDatabase zkDB = new ZKDatabase(txnLog);
        JvmPauseMonitor jvmPauseMonitor = null;
//        if (config.jvmPauseMonitorToRun) {
//            jvmPauseMonitor = new JvmPauseMonitor(config);
//        }
        zkServer = new ZooKeeperServer(jvmPauseMonitor, txnLog,
                                       config.getTickTime(), config.getMinSessionTimeout(),
                                       config.getMaxSessionTimeout(), config.getClientPortListenBacklog(),
                                       zkDB, null);
    }

    public void start() {
        // See ZooKeeperServerMain.runFromConfig
        LOG.info("Starting server");
        boolean needStartZKServer = true;
        LogCtl.disable("org.apache.zookeeper.server.ZooKeeperServer");
        // ZooKeeperServer logs an error because ZooKeeperServerShutdownHandler not set but that is not an accessible class.
        try {
            if (config.getClientPortAddress() != null) {
                cnxnFactory = ServerCnxnFactory.createFactory();
                cnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns(), -1, false);
                cnxnFactory.startup(zkServer);
                // zkServer has been started. So we don't need to start it again in secureCnxnFactory.
                needStartZKServer = false;
            }
            if (config.getSecureClientPortAddress() != null) {
                secureCnxnFactory = ServerCnxnFactory.createFactory();
                secureCnxnFactory.configure(config.getSecureClientPortAddress(), config.getMaxClientCnxns(), -1, true);
                secureCnxnFactory.startup(zkServer, needStartZKServer);
            }

            if ( needStartZKServer )
                zkServer.startup();
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        } catch (InterruptedException e) {
            // Note from ZookeeperServerMain: "warn, but generally this is ok"
            LOG.warn("Server interrupted", e);
        } finally {
            LogCtl.setLevel("org.apache.zookeeper.server.ZooKeeperServer", "WARN");
        }
    }

    public void join() {
        if (cnxnFactory != null) {
            try {
                cnxnFactory.join();
            }
            catch (InterruptedException e) {
                // Note from ZookeeperServerMain: warn, but generally this is ok
                LOG.warn("Server join interrupted", e);
            }
        }
    }


    public void stop() {
        // Tail of ZooKeeperServerMain.runFromConfig
        shutdown();
    }
    /**
     * Shutdown the serving instance
     */
    protected void shutdown() {
        LogCtl.disable("org.apache.zookeeper.server.ZooKeeperServer");
        try {
//            if (zkServer.canShutdown()) {
            zkServer.shutdown(true);
            if (containerManager != null) {
                containerManager.stop();
            }
            if (cnxnFactory != null) {
                cnxnFactory.shutdown();
            }
            if (secureCnxnFactory != null) {
                secureCnxnFactory.shutdown();
            }
            if (adminServer != null) {
                adminServer.shutdown();
            }
        } catch (AdminServerException e) {
            LOG.warn("Problem stopping AdminServer", e);
        } finally {
            LogCtl.setLevel("org.apache.zookeeper.server.ZooKeeperServer", "WARN");
        }
        if (txnLog != null) {
            try { txnLog.close(); }
            catch (IOException e) { e.printStackTrace(); }
        }

    }
}
