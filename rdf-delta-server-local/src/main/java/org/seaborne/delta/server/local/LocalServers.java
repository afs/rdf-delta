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

package org.seaborne.delta.server.local;

import java.nio.file.Path;

import org.apache.curator.framework.CuratorFramework;
import org.seaborne.delta.server.local.patchstores.zk.PatchStoreProviderZk;

/** Ways to construct a {@link LocalServer} */
public class LocalServers {
    
    // LocalServer.Builder?

    public static LocalServer create(LocalServerConfig configuration) {
        return LocalServer.create(configuration);
    }

    /** Create a {@link LocalServer} with a file-based {@link PatchStoreProvider}. */ 
    public static LocalServer createFile(String directory) {
        LocalServerConfig config = LocalServerConfig.create()
            .setProperty("delta.file", directory)
            .setLogProvider(DPS.PatchStoreFileProvider)
            .build();
        return create(config);
    }
    
    /** Create a {@link LocalServer} with a file-based {@link PatchStoreProvider}. */ 
    public static LocalServer createFile(Path dirPath) {
        return createFile(dirPath.toString());
    }

    /** Create a {@link LocalServer} with an in-memory {@link PatchStoreProvider}. */ 
    public static LocalServer createMem() {
        LocalServerConfig config = LocalServerConfig.create()
            .setLogProvider(DPS.PatchStoreMemProvider)
            .build();
        return create(config);
    }
    
    /** Create a {@link LocalServer} with an in-memory {@link PatchStoreProvider}. */ 
    public static LocalServer createZk(String connectionString) { 
        LocalServerConfig config = LocalServerConfig.create()
            .setLogProvider(DPS.PatchStoreMemProvider)
            .setProperty("delta.zk", connectionString)
            .build();
        return create(config);
    }
    
    /** Create a {@link LocalServer} using an existing {@link CuratorFramework}. */ 
    public static LocalServer createZk(CuratorFramework client) {
        LocalServerConfig config = LocalServerConfig.create()
            .setLogProvider(DPS.PatchStoreMemProvider)
            .setProperty("delta.zk", client.getZookeeperClient().getCurrentConnectionString())
            .build();
        PatchStore ps = new PatchStoreProviderZk(client).create(config);
        return LocalServer.create(ps, config); 
    }

    public static LocalServer createConf(String configFile) {
        LocalServerConfig config = LocalServerConfig.create()
            .parse(configFile)
            .build();
        return LocalServer.create(config);
    }
}
