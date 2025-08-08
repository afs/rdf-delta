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

import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.DeltaServer;
import org.seaborne.delta.server.local.*;
import org.seaborne.delta.server.local.patchstores.any.PatchStoreProviderAnyLocal;
import org.seaborne.delta.server.local.patchstores.file.PatchStoreProviderFile;
import org.seaborne.delta.server.local.patchstores.mem.PatchStoreProviderMem;
import org.seaborne.delta.server.local.patchstores.rdb.PatchStoreProviderRocks;
import org.slf4j.Logger;

public /*package*/ class ServerBuildLib {

    private static Logger LOG = Delta.DELTA_CONFIG_LOG;

    /**
     * Build a {@link DeltaServer}.
     */
    public static DeltaServer build(DeltaServerConfig deltaServerConfig) {
        // This could be moved into DeltaServer.start.
        Supplier<LocalServerConfig> startup = ()->{
            // Further setup of the JVM.
            startEnvirionment(deltaServerConfig);
            return setupLocalServerConfig(deltaServerConfig);
        };
        if ( deltaServerConfig.serverPort == null && deltaServerConfig.jettyConf == null )
            throw new DeltaConfigException("No and no Jetty config file");
        if ( deltaServerConfig.serverPort != null && deltaServerConfig.jettyConf != null )
            throw new DeltaConfigException("Both port and Jetty config file provided");

        int serverPort = (deltaServerConfig.serverPort != null) ? deltaServerConfig.serverPort : -1;
        DeltaServer deltaServer = buildServer(serverPort, deltaServerConfig.jettyConf, startup);
        return deltaServer;
    }

    /** {@link DeltaServerConfig} to {@link LocalServerConfig}. */
    public static LocalServerConfig setupLocalServerConfig(DeltaServerConfig deltaServerConfig) {
        PatchStoreProvider psp;
        LocalServerConfig localServerConfig;
        String providerLabel;

        switch (deltaServerConfig.provider) {
            case FILE :
                psp = installProvider(new PatchStoreProviderFile());
                localServerConfig = LocalServers.configFile(deltaServerConfig.fileBase, deltaServerConfig.jettyConf);
                providerLabel = "file["+deltaServerConfig.fileBase+"]";
                break;
            case ROCKS :
                psp = installProvider(new PatchStoreProviderRocks());
                localServerConfig = LocalServers.configRDB(deltaServerConfig.fileBase, deltaServerConfig.jettyConf);
                providerLabel = "rdb["+deltaServerConfig.fileBase+"]";
                break;
            case LOCAL:
                psp = installProvider(new PatchStoreProviderAnyLocal());
                localServerConfig = LocalServers.configLocal(deltaServerConfig.fileBase, deltaServerConfig.jettyConf);
                providerLabel = "local["+deltaServerConfig.fileBase+"]";
                break;
            case MEM :
                psp = installProvider(new PatchStoreProviderMem());
                localServerConfig = LocalServers.configMem(deltaServerConfig.jettyConf);
                providerLabel = "mem";
                break;
                           default :
                throw new DeltaConfigException("Unrecognized provider: "+deltaServerConfig.provider);
        }
        LOG.debug("Setup for provider: "+providerLabel);

        return localServerConfig;
    }

    private static void startEnvirionment(DeltaServerConfig deltaServerConfig) {}

    private static PatchStoreProvider installProvider(PatchStoreProvider psp) {
        if ( ! PatchStoreMgr.isRegistered(psp.getType()) )
            PatchStoreMgr.register(psp);
        return psp;
    }

    // --> DeltaServer.start()
    private static DeltaServer buildServer(int port, String jettyConfigFile, Supplier<LocalServerConfig> startup) {
        LocalServerConfig localServerConfig = startup.get();
        // Scope for further properties.
        Properties properties = new Properties();
        if ( ! properties.isEmpty() )
            localServerConfig = LocalServerConfig.create(localServerConfig).setProperties(properties).build();

        LocalServer server = LocalServer.create(localServerConfig);
        DeltaLink link = DeltaLinkLocal.connect(server);

        DeltaServer deltaServer;
        if ( jettyConfigFile != null ) {
            FmtLog.info(LOG, "Delta Server jetty config=%s", jettyConfigFile);
            deltaServer = DeltaServer.create(jettyConfigFile, link);
        } else {
            FmtLog.info(LOG, "Delta Server port=%d", port);
            deltaServer = DeltaServer.create(port, link);
        }
        return deltaServer;
    }
}
