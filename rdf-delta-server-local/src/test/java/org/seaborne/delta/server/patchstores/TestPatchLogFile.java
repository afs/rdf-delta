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

package org.seaborne.delta.server.patchstores;

import org.apache.jena.atlas.lib.FileOps;
import org.junit.After;
import org.junit.Before;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.delta.server.local.*;
import org.seaborne.delta.server.local.patchstores.file2.PatchStoreProviderFile;
import org.seaborne.delta.server.local.patchstores.filestore.FileStore;

public class TestPatchLogFile extends AbstractTestPatchLog {

    private static final String LOG = "target/test";
    private static final LocalServerConfig config = LocalServers.configFile(LOG);
    private PatchStore patchStore;
    private PatchLog patchLog;

    @Before public void before() {
        FileStore.resetTracked();
        FileOps.ensureDir(LOG);
        FileOps.clearAll(LOG);
    }

    @After public void after() {
        patchLog.release();
    }

    @Override
    protected PatchLog patchLog() {
        PatchStoreProviderFile psp = (PatchStoreProviderFile)PatchStoreMgr.getPatchStoreProvider(DPS.PatchStoreFileProvider);
        DataSourceDescription dsd = new DataSourceDescription(Id.create(), "ABC", "http://test/ABC");
        patchStore = new PatchStoreProviderFile().create(config);
        patchStore.initialize(new DataSourceRegistry("X"), config);
        patchLog = patchStore.createLog(dsd);
        return patchLog;
    }
}
