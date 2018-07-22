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

import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.server.system.DeltaSystem;

/** Ways to construct a {@link LocalServer} */
public class LocalServers {
    static { DeltaSystem.init(); } 

    public static LocalServer create(LocalServerConfig configuration) {
        return LocalServer.create(configuration);
    }

    /** {@link LocalServerConfig} for a {@link LocalServer} with a file-based patch store. */ 
    public static LocalServerConfig configFile(String directory) {
        return LocalServerConfig.create()
            .setProperty(DeltaConst.pDeltaFile, directory)
            .setLogProvider(DPS.PatchStoreFileProvider)
            .build();
    }

    /** {@link LocalServerConfig} for a {@link LocalServer} with a memory-based patch store. */ 
    public static LocalServerConfig configMem() {
        return LocalServerConfig.create()
            .setLogProvider(DPS.PatchStoreMemProvider)
            .build();
    }

    /** {@link LocalServerConfig} for a {@link LocalServer} with a zookeeper-based patch store. */ 
    public static LocalServerConfig configZk(String connectionString) { 
        LocalServerConfig.Builder builder = LocalServerConfig.create()
            .setLogProvider(DPS.PatchStoreZkProvider);
        if ( connectionString != null )
            builder.setProperty(DeltaConst.pDeltaZk, connectionString);
        return builder.build();
    }

    /** Create a {@link LocalServer} with a file-based {@link PatchStore}. */ 
    public static LocalServer createFile(String directory) {
        return create(configFile(directory));
    }
    
    /** Create a {@link LocalServer} with a file-based {@link PatchStore}. */ 
    public static LocalServer createFile(Path dirPath) {
        return createFile(dirPath.toString());
    }

    /** Create a {@link LocalServer} with an in-memory {@link PatchStore}. */ 
    public static LocalServer createMem() {
        return create(configMem());
    }
    
    /** Create a {@link LocalServer} with a Apache ZooKeeper based {@link PatchStore}. */ 
    public static LocalServer createZk(String connectionString) { 
        return create(configZk(connectionString));
    }
    
//    /** Create a {@link LocalServer} using an existing {@link CuratorFramework}.
//     * Special case; normally, each {@link PatchStoreZk} has it's own {@code CuratorFramework}.
//     */ 
//    public static LocalServer createZk(CuratorFramework client) {
//        LocalServerConfig config = configZk(null);
//        PatchStore ps = new PatchStoreProviderZk(client).create(config);
//        return LocalServer.create(ps, config); 
//    }

    public static LocalServer createConf(String configFile) {
        LocalServerConfig config = LocalServerConfig.create()
            .parse(configFile)
            .build();
        return LocalServer.create(config);
    }
}
