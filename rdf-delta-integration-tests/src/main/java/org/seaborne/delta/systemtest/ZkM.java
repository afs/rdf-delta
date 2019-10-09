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

import java.io.IOException;
import java.util.Properties;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.seaborne.delta.zk.ZkS;

public class ZkM {
    /** 
     * Run an ensemble.
     * Untested.
     * 
     * @param basePort
     * @param dataDir
     * @see ZkS#runZookeeperServer(String)
     */
    public static void runZookeeperEnsemble(int basePort, String dataDir) {
        // Servers are: zk1, zk2, zk3.
        server(1, basePort, dataDir);
        server(2, basePort, dataDir);
        server(3, basePort, dataDir);
    }

    /** ZooKeeper server {@code i} */
    private static void server(int i, int basePort, String rootDir) {
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
        if ( rootDir != null )
            dir = rootDir+"/"+dir;
        
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
