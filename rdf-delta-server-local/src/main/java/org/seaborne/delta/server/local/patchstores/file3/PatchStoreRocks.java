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

package org.seaborne.delta.server.local.patchstores.file3;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchLog;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchStoreProvider;
import org.seaborne.delta.server.local.patchstores.file.CfgFile;

public class PatchStoreRocks extends PatchStore {
    /*
     *  / server root
     *    delta.cfg
     *    / source
     *       / source.cfg
     *       / database /
     *
     *   RocksDB: index:
     *     (version, id)
     *     (id, PatchInfo)  ?? for PatchLogIndex.getPatchInfo(Id)
     *   RocksDB: patch storage
     *     (id, patch)
     */

    // Singletons.
    // "static" so two PatchStoreRocks go to the same databases.
    private static Map<Id, RocksDatabase> databases = new ConcurrentHashMap<>();

    private Path patchLogDirectory = null;

    /*package*/ PatchStoreRocks(String patchLogDirectory, PatchStoreProvider provider) {
        super(provider);
        Objects.requireNonNull(patchLogDirectory);
        this.patchLogDirectory = Paths.get(patchLogDirectory);
    }

    public static void resetTracked() {
        databases.values().forEach(RocksDatabase::close);
        databases.clear();
    }

    @Override
    protected void startStore() {}

    @Override
    protected void closeStore() {}

    @Override
    protected void deleteStore() {}

    @Override
    protected List<DataSourceDescription> initialize(LocalServerConfig config) {
        return CfgFile.scanForLogs(patchLogDirectory);
    }

    @Override
    protected PatchLog newPatchLog(DataSourceDescription dsd) {
        Id id = dsd.getId();
        databases.computeIfAbsent(id, x->{
            Path fileStoreDir = patchLogDirectory.resolve(dsd.getName());
            if ( ! Files.exists(fileStoreDir) ) {
                CfgFile.setupDataSourceByFile(patchLogDirectory, this, dsd);
//                try { Files.createDirectory(fileStoreDir); }
//                catch (IOException ex) { throw IOX.exception(ex); }
            }
            Path dbPath = fileStoreDir.resolve(RocksConst.databaseFilename).toAbsolutePath();
            RocksDatabase db = new RocksDatabase(dbPath);
            return db;
        });
        // The database will be picked up by newPatchLogIndex, newPatchStorage
        // calls in PatchStoreProviderFileRocks
        PatchLog newPatchLog = newPatchLogFromProvider(dsd);
        return newPatchLog;
    }

    public RocksDatabase getDatabase(Id id) {
        return databases.get(id);
    }

    @Override
    protected void delete(PatchLog patchLog) {
        PatchStoreRocks patchStoreFile = (PatchStoreRocks)patchLog.getPatchStore();
        Id id = patchLog.getDescription().getId();
        RocksDatabase database = databases.remove(id);
        Path p = database.getPath();
        IOX.deleteAll(p);
    }

}
