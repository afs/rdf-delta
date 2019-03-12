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

package org.seaborne.delta.server.local.patchstores.file;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchLog;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchStoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatchStoreFile extends PatchStore {
    private static Logger LOG = LoggerFactory.getLogger(PatchStoreFile.class);

    /*   Server Root
     *      delta.cfg
     *      /NAME ... per DataSource.
     *          /source.cfg
     *          /Log -- patch on disk (optional)
     *          /data -- TDB database (optional)
     *          /disabled -- if this file is present, then the datasource is not accessible.
     */

    private final Path serverRoot;

    public PatchStoreFile(String location, PatchStoreProvider provider) {
        this(Paths.get(location), provider);
    }

    public PatchStoreFile(Path location, PatchStoreProvider provider) {
        super(provider);
        IOX.ensureDirectory(location);
        this.serverRoot = location;
    }

    @Override
    protected List<DataSourceDescription> initialize(LocalServerConfig config) {
        return CfgFile.scanForLogs(serverRoot, this);
    }

    @Override
    protected PatchLog newPatchLog(DataSourceDescription dsd) {
        Path patchLogArea = serverRoot.resolve(dsd.getName());
        if ( ! Files.exists(patchLogArea) )
            CfgFile.setupDataSourceByFile(serverRoot, this, dsd);
        Location loc = Location.create(patchLogArea.toString());
        PatchLog pLog = PatchLogFile.attach(dsd, this, loc);
        return pLog;
    }

    @Override
    protected void delete(PatchLog patchLog) {
        Path p = ((PatchLogFile)patchLog).getFileStore().getPath();
        patchLog.delete();
    }

    @Override
    protected void startStore() {}

    @Override
    protected void closeStore() { }

    @Override
    protected void deleteStore() { }
}
