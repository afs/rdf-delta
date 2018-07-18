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

import java.io.IOException;

import org.apache.jena.atlas.logging.LogCtl;
import org.apache.zookeeper.server.ContainerManager;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Baees on ZooKeeperServerMain.
 */
public class ZooKeeperHelper {
    private static final Logger LOG =
        LoggerFactory.getLogger(ZooKeeperHelper.class);

    private static final String USAGE =
        "Usage: ZooKeeperServerMain configfile | port datadir [ticktime] [maxcnxns]";

    // ZooKeeper server supports two kinds of connection: unencrypted and encrypted.
    private ServerCnxnFactory cnxnFactory;
    private ServerCnxnFactory secureCnxnFactory;
    private ContainerManager containerManager;

    private ZooKeeperServer zkServer;
    private FileTxnSnapLog txnLog; 
    private AdminServer adminServer;

    private ServerConfig config;

    public ZooKeeperHelper(ServerConfig config) {
        this.config = config;
    }
    
    public void setupFromConfig() {
            // Note that this thread isn't going to be doing anything else,
            // so rather than spawning another thread, we will just call
            // run() in this thread.
            // create a file logger url from the command line args
            try {
                txnLog = new FileTxnSnapLog(config.getDataLogDir(), config.getDataDir());
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            zkServer = new ZooKeeperServer(txnLog, config.getTickTime(), config.getMinSessionTimeout(), config.getMaxSessionTimeout(), null);
            // Can't. zkServer.registerServerShutdownHandler(cls.ca
//            // Start Admin server
//            adminServer = AdminServerFactory.createAdminServer();
//            adminServer.setZooKeeperServer(zkServer);
//            adminServer.start();
    }
    
    public void start() {
//        try{
        LOG.info("Starting server");
        boolean needStartZKServer = true;
        LogCtl.disable("org.apache.zookeeper.server.ZooKeeperServer");
        // ZooKeeperServer logs an error because ZooKeeperServerShutdownHandler not set but that is not an accessible class. 
        try {
            if (config.getClientPortAddress() != null) {
                cnxnFactory = ServerCnxnFactory.createFactory();
                cnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns(), false);
                cnxnFactory.startup(zkServer);
                // zkServer has been started. So we don't need to start it again in secureCnxnFactory.
                needStartZKServer = false;
            }
            if (config.getSecureClientPortAddress() != null) {
                secureCnxnFactory = ServerCnxnFactory.createFactory();
                secureCnxnFactory.configure(config.getSecureClientPortAddress(), config.getMaxClientCnxns(), true);
                secureCnxnFactory.startup(zkServer, needStartZKServer);
            }

            if ( needStartZKServer )
                zkServer.startup();
            // join.
//            if (cnxnFactory != null) {
//                cnxnFactory.join();
//            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            // warn, but generally this is ok
            LOG.warn("Server interrupted", e);
        } finally {
            LogCtl.setLevel("org.apache.zookeeper.server.ZooKeeperServer", "WARN");
        }

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

    // VisibleForTesting
    ServerCnxnFactory getCnxnFactory() {
        return cnxnFactory;
    }
}
