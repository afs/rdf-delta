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

package org.seaborne.delta.server.local;

import java.nio.file.Path;

import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.server.Provider;
import org.seaborne.delta.server.system.DeltaSystem;

/** Ways to construct a {@link LocalServer} */
public class LocalServers {
    static { DeltaSystem.init(); }

    public static LocalServer create(LocalServerConfig configuration) {
        return LocalServer.create(configuration);
    }

    /** {@link LocalServerConfig} for a {@link LocalServer} with a local storage patch store. */
    public static LocalServerConfig configLocal(String directory) {
        return LocalServerConfig.create()
            .setProperty(DeltaConst.pDeltaStore, directory)
            .setLogProvider(Provider.LOCAL)
            .build();
    }

    /** {@link LocalServerConfig} for a {@link LocalServer} with a file-based patch store. */
    public static LocalServerConfig configFile(String directory) {
        return LocalServerConfig.create()
            .setProperty(DeltaConst.pDeltaStore, directory)
            .setLogProvider(Provider.FILE)
            .build();
    }

    /** {@link LocalServerConfig} for a {@link LocalServer} with a RockDB-based patch store. */
    public static LocalServerConfig configRDB(String directory) {
        return LocalServerConfig.create()
            .setProperty(DeltaConst.pDeltaStore, directory)
            .setLogProvider(Provider.ROCKS)
            .build();
    }

    /** {@link LocalServerConfig} for a {@link LocalServer} with a memory-based patch store. */
    public static LocalServerConfig configMem() {
        return LocalServerConfig.create()
            .setLogProvider(Provider.MEM)
            .build();
    }

    /**
     * {@link LocalServerConfig} for a {@link LocalServer} with a zookeeper-based index
     * and zookeeper-based index patch store.
     */
    public static LocalServerConfig configZk(String connectionString) {
        LocalServerConfig.Builder builder = LocalServerConfig.create()
            .setLogProvider(Provider.ZKZK);
        if ( connectionString != null )
            builder.setProperty(DeltaConst.pDeltaZk, connectionString);
        return builder.build();
    }

    /** Create a {@link LocalServer} with a mixed local provider {@link PatchStore}. */
    public static LocalServer createLocal(String directory) {
        return create(configLocal(directory));
    }

    /** Create a {@link LocalServer} with a mixed local provider {@link PatchStore}. */
    public static LocalServer createLocal(Path dirPath) {
        return createLocal(dirPath.toString());
    }

    /** Create a {@link LocalServer} with a file-based {@link PatchStore}. */
    public static LocalServer createFile(String directory) {
        return create(configFile(directory));
    }

    /** Create a {@link LocalServer} with a file-based {@link PatchStore}. */
    public static LocalServer createFile(Path dirPath) {
        return createFile(dirPath.toString());
    }

    /** Create a {@link LocalServer} with a RocksDB-based {@link PatchStore}. */
    public static LocalServer createRDB(String directory) {
        return create(configRDB(directory));
    }

    /** Create a {@link LocalServer} with a RocksDB-based {@link PatchStore}. */
    public static LocalServer createRDB(Path dirPath) {
        return createRDB(dirPath.toString());
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

    public static LocalServer createFromConf(String configFile) {
        LocalServerConfig config = LocalServerConfig.create()
            .parse(configFile)
            .build();
        return LocalServer.create(config);
    }
}
