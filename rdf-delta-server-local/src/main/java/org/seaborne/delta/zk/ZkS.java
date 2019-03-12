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

import java.util.Properties;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Library of Zookeeper server operations.
 * @see Zk 
 */
public class ZkS {
    private final static Logger LOG = LoggerFactory.getLogger(ZkS.class);
    
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
    
    /* 
     * Create a real ZookeeperServer (not a Curator test server) as stand-alone with persistent state
     * and return a wrapper.
     * The server has not been started.
     */
    public static ZooServer runZookeeperServer(int port, String dataDir) {  
        String[] args = {Integer.toString(port), dataDir};
        return runZookeeperServer(args);
    }

    /* 
     * Create a real ZookeeperServer (not a Curator test server) using the provided configuration file.
     * The server has not been started.
     */
    public static ZooServer runZookeeperServer(String zooConfFile) {
        zkSystemProps();
        ServerConfig config = new ServerConfig();
        try { 
            config.parse(zooConfFile);
        } catch (ConfigException e) {
            FmtLog.error(LOG, "Error in Zookeeper configuration file '%s': %s", zooConfFile, e.getMessage());
            throw new IllegalArgumentException(e);
        }
        ZooServer zksm = new ZooServer(config);
        zksm.setupFromConfig();
        return zksm;
    }
    
    /* 
     * Create a real ZookeeperServer using the provided arguments for the {@link ServerConfig}.
     * The server has not been started.
     */
    private static ZooServer runZookeeperServer(String[] args) {
        zkSystemProps();
        ServerConfig config = new ServerConfig();
        config.parse(args);
        ZooServer zksm = new ZooServer(config);
        zksm.setupFromConfig();
        return zksm;
    }
}
