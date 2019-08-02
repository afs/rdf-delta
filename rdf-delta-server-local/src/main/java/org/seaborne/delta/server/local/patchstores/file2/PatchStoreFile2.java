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

package org.seaborne.delta.server.local.patchstores.file2;

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
import org.seaborne.delta.server.local.filestore.FileStore;
import org.seaborne.delta.server.local.patchstores.file.CfgFile;

public class PatchStoreFile2 extends PatchStore {
    /*   Server Root
     *      delta.cfg
     *      /NAME ... per DataSource.
     *          /source.cfg
     *          /Log -- patch on disk (optional)
     *          /data -- TDB database (optional)
     *          /disabled -- if this file is present, then the datasource is not accessible.
     */
    private static final String patchBasename = "patch";

    // Singletons.
    // Should  patchLogDirectory->cache

    private static Map<Id, FilePatchIdx> filePatchIdxs = new ConcurrentHashMap<>();

    private Path patchLogDirectory = null;

    /*package*/ PatchStoreFile2(String patchLogDirectory, PatchStoreProvider provider) {
        super(provider);
        Objects.requireNonNull(patchLogDirectory);
        this.patchLogDirectory = Paths.get(patchLogDirectory);
    }

    public static void resetTracked() {
        // [FILE2]
        filePatchIdxs.clear();
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
        filePatchIdxs.computeIfAbsent(id, x->{
            Path fileStoreDir = patchLogDirectory.resolve(dsd.getName());
            if ( ! Files.exists(fileStoreDir) )
                CfgFile.setupDataSourceByFile(patchLogDirectory, this, dsd);
            FileStore fileStore = FileStore.attach(fileStoreDir, patchBasename);
            return FilePatchIdx.create(fileStore);
        });
        // The FilePatchIdx will be picked up by newPatchLogIndex, newPatchStorage
        // calls in PatchStoreProviderFile2.
        PatchLog newPatchLog = newPatchLogFromProvider(dsd);
        return newPatchLog;
    }

    public FilePatchIdx getPatchLogFile(Id id) {
        return filePatchIdxs.get(id);
    }

    @Override
    protected void delete(PatchLog patchLog) {
        PatchStoreFile2 patchStoreFile = (PatchStoreFile2)patchLog.getPatchStore();
        Id id = patchLog.getDescription().getId();
        FilePatchIdx filePatchIdx = filePatchIdxs.remove(id);
        Path p = filePatchIdx.getPath();
        IOX.deleteAll(p);
    }

}
