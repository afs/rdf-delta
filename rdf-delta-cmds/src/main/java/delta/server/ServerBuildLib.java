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

package delta.server;

import java.util.Properties;
import java.util.function.Supplier;

import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.Provider;
import org.seaborne.delta.server.http.DeltaServer;
import org.seaborne.delta.server.local.*;
import org.seaborne.delta.server.local.patchstores.file2.PatchStoreProviderFile;
import org.seaborne.delta.server.local.patchstores.mem.PatchStoreProviderMem;
import org.seaborne.delta.server.local.patchstores.rdb.PatchStoreProviderRocks;
import org.seaborne.delta.server.local.patchstores.zk.PatchStoreProviderZk;
import org.seaborne.delta.server.s3.PatchStoreProviderZkS3;
import org.seaborne.delta.server.s3.S3;
import org.seaborne.delta.server.s3.S3Config;
import org.seaborne.delta.zk.ZkS;
import org.seaborne.delta.zk.ZooServer;
import org.slf4j.Logger;

public /*package*/ class ServerBuildLib {

    private static Logger LOG = Delta.DELTA_CONFIG_LOG;

    /**
     * Build a {@link DeltaServer}. This will create and run a Zookeeper server in the local that have been requested.
     */
    public static DeltaServer build(DeltaServerConfig deltaServerConfig) {
        // Curator needs ZK running. PatchStore when created reads the persistent state.
        // This could be moved into DeltaServer.start.
        Supplier<LocalServerConfig> startup = ()->{
            // Further setup of the JVM. e.g. start in-process zookeeper.
            startEnvirionment(deltaServerConfig);
            return setupLocalServerConfig(deltaServerConfig);
        };
        DeltaServer deltaServer = buildServer(deltaServerConfig.serverPort, startup);
        return deltaServer;
    }

    private static LocalServerConfig setupLocalServerConfig(DeltaServerConfig deltaServerConfig) {
        PatchStoreProvider psp;
        LocalServerConfig localServerConfig;
        String providerLabel;

        switch (deltaServerConfig.provider) {
            case FILE :
                psp = installProvider(new PatchStoreProviderFile());
                localServerConfig = LocalServers.configFile(deltaServerConfig.fileBase);
                providerLabel = "file["+deltaServerConfig.fileBase+"]";
                break;
            case ROCKS :
                psp = installProvider(new PatchStoreProviderRocks());
                localServerConfig = LocalServers.configRDB(deltaServerConfig.fileBase);
                providerLabel = "rdb["+deltaServerConfig.fileBase+"]";
                break;
            case MEM :
                psp = installProvider(new PatchStoreProviderMem());
                localServerConfig = LocalServers.configMem();
                providerLabel = "mem";
                break;
            case ZKZK :
                psp = installProvider(new PatchStoreProviderZk());
                localServerConfig = serverConfigZookeeper(deltaServerConfig);
                providerLabel = "zookeeper";
                break;
            case ZKS3 :
                psp = installProvider(new PatchStoreProviderZkS3());
                localServerConfig = serverConfigZookeeper(deltaServerConfig);
                localServerConfig = setupS3(deltaServerConfig, localServerConfig);
                providerLabel = "zookeeper+s3";
                break;
            default :
                throw new DeltaConfigException("Unrecognized provider: "+deltaServerConfig.provider);
        }
        LOG.debug("Setup for provider: "+providerLabel);

        localServerConfig.jettyConf = deltaServerConfig.jettyConf;
        return localServerConfig;
    }

    private static void startEnvirionment(DeltaServerConfig deltaServerConfig) {
        if ( isZookeeper(deltaServerConfig) )
            runZookeeper(deltaServerConfig);
    }

    private static PatchStoreProvider installProvider(PatchStoreProvider psp) {
        if ( ! PatchStoreMgr.isRegistered(psp.getProvider()) )
            PatchStoreMgr.register(psp);
        return psp;
    }

    /** Return a {@link LocalServerConfig} for zookeeper usage */
    private static LocalServerConfig serverConfigZookeeper(DeltaServerConfig config) {
        // Allocate zookeeper
        if ( config.zkConnectionString == null )
            throw new DeltaConfigException("No connection string for Zookeeper");

        // Set connection string when the test server starts.
        //        if ( config.zkMode == ZkMode.MEM ) {
        //            if ( config.zkPort == null )
        //                config.zkPort = WebLib.choosePort();
        //            config.zkConnectionString = "localhost:"+config.zkPort;
        //        }

        if ( config.zkConnectionString == null )
            throw new DeltaConfigException("No connection string for ZooKeeper");
        // If zk+S3, there isn't a provider name set yet.
        LocalServerConfig localServerConfig = LocalServers.configZk(config.zkConnectionString);
        return localServerConfig;
    }

    private static boolean isZookeeper(DeltaServerConfig config) {
        return config.provider == Provider.ZKZK || config.provider == Provider.ZKS3;
    }

    private static boolean localZookeeper(DeltaServerConfig config) {
        switch (config.zkMode) {
            case MEM :
            case QUORUM :
            case SINGLE :
                return true;
            default :
            case NONE :
            case EXTERNAL :
                return false;
        }
    }

    private static void runZookeeper(DeltaServerConfig config) {
        if ( !localZookeeper(config) )
            return ;

        switch(config.zkMode) {
            case NONE : throw new InternalErrorException();
            case SINGLE : {
                FmtLog.info(LOG, "Zookeeper(single): Connection string: %s", config.zkConnectionString);
                ZooServer zs = ZkS.runZookeeperServer(config.zkPort, config.zkData);
                zs.start();
                break;
            }
            case QUORUM : {
                FmtLog.info(LOG, "Zookeeper: Connection string: %s", config.zkConnectionString);
                if ( config.zkConf == null )
                    throw new InternalErrorException("config.zkConf == null");
                ZooServer.quorumServer(config.zkConf);
                break;
            }
            case MEM : {
                config.zkConnectionString = ZkJVM.startZooJVM();
                FmtLog.info(LOG, "Zookeeper(mem): Connection string: %s", config.zkConnectionString);
                break;
            }
            default :
                break;
        }
    }

    private static LocalServerConfig setupS3(DeltaServerConfig config, LocalServerConfig localServerConfig) {
        // Take the provided Zookeeper LocalServerConfig and create a new one that is same
        // zookeeper index provider with S3 as the storage.
        S3Config cfg = S3Config.create()
          .bucketName(config.s3BucketName)
          .region(config.s3Region)
          .endpoint(config.s3Endpoint)
          .credentialsFile(config.s3CredentialsFile)
          .credentialsProfile(config.s3CredentialsProfile)
          .build();
        return S3.configZkS3(localServerConfig, cfg);
    }

    // --> DeltaServer.start()
    private static DeltaServer buildServer(int port, Supplier<LocalServerConfig> startup) {
        LocalServerConfig localServerConfig = startup.get();
        // Scope for further properties.
        Properties properties = new Properties();
        if ( ! properties.isEmpty() )
            localServerConfig = LocalServerConfig.create(localServerConfig).setProperties(properties).build();

        LocalServer server = LocalServer.create(localServerConfig);
        DeltaLink link = DeltaLinkLocal.connect(server);

        DeltaServer deltaServer;
        if ( localServerConfig.jettyConf != null ) {
            FmtLog.info(LOG, "Delta Server jetty config=%s", localServerConfig.jettyConf);
            deltaServer = DeltaServer.create(localServerConfig.jettyConf, link);
        } else {
            FmtLog.info(LOG, "Delta Server port=%d", port);
            deltaServer = DeltaServer.create(port, link);
        }
        return deltaServer;
    }
}
