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
import java.util.Properties;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;

public class ZkM {
    /* 
     * Create a real ZookeeperServer as stand-alone with persistent state
     * and return a wrapper.
     * The server has not been started.
     */
    public static ZooKeeperHelper runZookeeperServer(int port, String dataDir) {  
        zkSystemProps();
        String[] args = {Integer.toString(port), dataDir};
        ServerConfig config = new ServerConfig();
        config.parse(args);
        ZooKeeperHelper zksm = new ZooKeeperHelper(config);
        zksm.setupFromConfig();
        return zksm;
    }
    
    // These are all system properties only.
    public static void zkSystemProps() {
        Properties props = System.getProperties();
        // See org.apache.zookeeper.jmx.ManagedUtil
        props.setProperty("zookeeper.jmx.log4j.disable", "true");
        
        // See org.apache.zookeeper.server.admin.AdminServerFactory
        props.setProperty("zookeeper.admin.enableServer", "false");
        
        // See org.apache.zookeeper.server.NIOServerCnxnFactory
        props.setProperty("zookeeper.nio.numSelectorThreads", "1");
        props.setProperty("zookeeper.nio.numWorkerThreads",   "4");
        // "zookeeper.nio.directBufferBytes"
        // "zookeeper.nio.shutdownTimeout"
    }
    
    // Run an ensemble.
    // Untested.
    public static void runZookeeperEnsemble(int basePort, String dataDir) {
        // Servers are: zk1, zk2, zk3.
        server(1, basePort, dataDir);
        server(2, basePort, dataDir);
        server(3, basePort, dataDir);
    }

    /** ZooKeeper server {@code i} */
    private static void server(int i, int basePort, String dataDir) {
        /*
tickTime=2000
initLimit=5
syncLimit=2
dataDir=/home/afs/ASF/rdf-delta-dev/zk/zk1/ZkData
clientPort=2181
adminPort=2191
admin.enableServer=false
server.1=localhost:2281:3381
server.2=localhost:2282:3382
server.3=localhost:2283:3383
         */
        /* The complete list of static properties: 
        // dataDir
        // dataLogDir
        // clientPort
        // localSessionsEnabled
        // localSessionsUpgradingEnabled
        // clientPortAddress
        // secureClientPort
        // secureClientPortAddress
        // tickTime
        // maxClientCnxns
        // minSessionTimeout
        // maxSessionTimeout
        // initLimit
        // syncLimit
        // electionAlg
        // quorumListenOnAllIPs
        // peerType: "observer", "participant"
        // syncEnabled
        // dynamicConfigFile
        // autopurge.snapRetainCount
        // autopurge.purgeInterval
        // standaloneEnabled
        // reconfigEnabled
         */
        
        
        int clientPort = 2180 + i;
        int baseSysPort = 2280;
        int baseElectionPort = 2380;
        String dir = "zk/zk"+i;
        
        Properties staticProperties = new Properties();
        Properties dynamicProperties = new Properties();
        staticProperties.setProperty("tickTime", "2000");
        staticProperties.setProperty("dataDir", dir);
        staticProperties.setProperty("initLimit", "5") ;
        staticProperties.setProperty("syncLimit", "2") ;
        staticProperties.setProperty("clientPort", Integer.toString(clientPort));
        for ( int j = 1 ; i <=3 ; i++ )
            dynamicProperties.setProperty("server."+j, "localhost:"+(baseSysPort+j)+":"+(baseElectionPort+j) );
        try {
            QuorumPeerConfig c = new QuorumPeerConfig();
            c.parseProperties(staticProperties);
            QuorumVerifier qv = QuorumPeerConfig.parseDynamicConfig(dynamicProperties, 3, true, false);
            QuorumPeerMain quorumPeer = new QuorumPeerMain();
            new Thread(()-> {
                try { 
                    quorumPeer.runFromConfig(c);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        catch (IOException e1) {
            e1.printStackTrace();
        }
        catch (ConfigException e1) {
            e1.printStackTrace();
        }
        
    }
}
