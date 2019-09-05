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

package org.seaborne.delta.server.local.patchstores.any;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.Id;
import org.seaborne.delta.server.Provider;
import org.seaborne.delta.server.local.*;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.delta.server.local.patchstores.file.PatchStoreFile;
import org.seaborne.delta.server.local.patchstores.filestore.FileArea;
import org.seaborne.delta.server.local.patchstores.rdb.PatchStoreRocks;
import org.seaborne.delta.server.local.patchstores.rdb.RocksConst;

//This class exists to handle newPatchLog.

/**
 * A {@link PatchStore} that create a local file-based or RocksDB-based {@link PatchLog}
 * by intercepting {@link #newPatchLog}.
 */
public class PatchStoreAnyLocal extends PatchStore {

    private final PatchStoreFile   patchStoreFile;
    private final PatchStoreRocks  patchStoreRocks;
    private final PatchStore       patchStoreDefaultNew;

    private final Path patchLogDirectory;

    public PatchStoreAnyLocal(String patchLogDirectory, PatchStoreProvider provider) {
        super(provider);
        Objects.requireNonNull(patchLogDirectory);
        this.patchLogDirectory = Paths.get(patchLogDirectory);
        patchStoreFile = new PatchStoreFile(patchLogDirectory, PatchStoreMgr.getPatchStoreProvider(Provider.FILE));
        patchStoreRocks = new PatchStoreRocks(patchLogDirectory, PatchStoreMgr.getPatchStoreProvider(Provider.ROCKS));
        patchStoreDefaultNew = patchStoreRocks;
    }

    @Override
    public void initialize(DataSourceRegistry dataSourceRegistry, LocalServerConfig config) {
        patchStoreFile.initialize(dataSourceRegistry, config);
        patchStoreRocks.initialize(dataSourceRegistry, config);
        super.initialize(dataSourceRegistry, config);
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
        Path fileStoreDir = patchLogDirectory.resolve(dsd.getName());
        PatchStore patchStore = choose(dsd, fileStoreDir);
        return patchStore.createLog(dsd);
    }

    private PatchStore choose(DataSourceDescription dsd, Path patchLogDir) {
        if ( ! Files.exists(patchLogDir) ) {
            return patchStoreDefaultNew;
        } else {
            // Rocks has a "rdb" directory.
            // See PatchStoreRocks.newPatchLog
            Path dbPath = patchLogDir.resolve(RocksConst.databaseFilename).toAbsolutePath();
            boolean rocks = Files.exists(dbPath);
            //System.out.println("choose: "+dsd+" Rocks="+rocks);
            return rocks ? patchStoreRocks : patchStoreFile;
        }
    }

    // Not called.
    @Override
    protected PatchLogIndex newPatchLogIndex(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration) {
        throw new DeltaException("PatchStoreAnyLocal.newPatchLogIndex called");
    }

    @Override
    protected PatchStorage newPatchStorage(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration) {
        throw new DeltaException("PatchStoreAnyLocal.newPatchStorage called");
    }

    @Override
    protected void delete(PatchLog patchLog) {
        throw new DeltaException("PatchStoreAnyLocal.delete called");
    }

    @Override
    protected void shutdownSub() {
        patchStoreFile.shutdown();
        patchStoreRocks.shutdown();
    }
}
