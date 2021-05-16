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

package org.seaborne.delta.server.local.patchstores.rdb;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchLog;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchStoreProvider;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.delta.server.local.patchstores.filestore.FileArea;

public class PatchStoreRocks extends PatchStore {
    /*
     *  / server root
     *    delta.cfg
     *    / NAME
     *       / source.cfg
     *       / logs / Rocks database
     *       / disabled
     *   RocksDB: index:
     *     (version, id)
     *     (id, PatchInfo)  ?? for PatchLogIndex.getPatchInfo(Id)
     *   RocksDB: patch storage
     *     (id, patch)
     */

    // Singletons.
    // "static" so two PatchStoreRocks go to the same databases.
    private static Map<Id, LogIndexRocks> logIndexes = new ConcurrentHashMap<>();

    private final Path patchLogDirectory;

    public PatchStoreRocks(String patchLogDirectory, PatchStoreProvider provider) {
        super(provider);
        Objects.requireNonNull(patchLogDirectory);
        this.patchLogDirectory = Paths.get(patchLogDirectory);
    }

    public static void resetTracked() {
        logIndexes.values().forEach(idx->idx.shutdown());
        logIndexes.clear();
    }

    /*package*/ LogIndexRocks getLogIndex(Id id) {
        return logIndexes.get(id);
    }

    @Override
    protected void initialize(LocalServerConfig config) {}

    @Override
    protected List<DataSourceDescription> initialDataSources() {
        return FileArea.scanForLogs(patchLogDirectory);
    }

    @Override
    protected PatchLog newPatchLog(DataSourceDescription dsd) {
        Id id = dsd.getId();
        logIndexes.computeIfAbsent(id, x->{
            Path fileStoreDir = patchLogDirectory.resolve(dsd.getName());
            if ( ! Files.exists(fileStoreDir) )
                FileArea.setupDataSourceByFile(patchLogDirectory, this, dsd);
            Path dbPath = fileStoreDir.resolve(RocksConst.databaseFilename).toAbsolutePath();
            RocksDatabase db = new RocksDatabase(dbPath);
            LogIndexRocks idx = new LogIndexRocks(db);
            return idx;
        });
        // The database will be picked up by newPatchLogIndex and newPatchStorage
        PatchLog newPatchLog = newPatchLogFromIndexAndStorage(dsd);
        return newPatchLog;
    }

    @Override
    protected PatchLogIndex newPatchLogIndex(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration) {
        PatchStoreRocks patchStoreRocks = (PatchStoreRocks)patchStore;
        LogIndexRocks rIdx = patchStoreRocks.getLogIndex(dsd.getId());
        return new PatchLogIndexRocks(rIdx);
    }

    @Override
    protected PatchStorage newPatchStorage(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration) {
        PatchStoreRocks patchStoreRocks = (PatchStoreRocks)patchStore;
        LogIndexRocks rIdx = patchStoreRocks.getLogIndex(dsd.getId());
        return new PatchStorageRocks(rIdx.database());
    }

    @Override
    protected void delete(PatchLog patchLog) {
        String logName = patchLog.getDescription().getName();
        Path patchLogArea = patchLogDirectory.resolve(logName);
        Id id = patchLog.getDescription().getId();
        LogIndexRocks idx = logIndexes.remove(id);
        idx.database().close();
        FileArea.retire(patchLogArea);
    }

    @Override
    protected void shutdownSub() {}

}
