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
import org.seaborne.delta.server.local.*;
import org.seaborne.delta.server.local.patchstores.file3.PatchStoreRocks;

public class TestPatchStoreRocks extends AbstractTestPatchStore {
    private static String DIR = "target/test/patch-store-file";

    @After public void afterPatchStoreRocks() {
        PatchStoreRocks.resetTracked();
    }


    @Override
    protected PatchStore patchStore(DataRegistry dataRegistry) {
        FileOps.ensureDir(DIR);
        FileOps.clearAll(DIR);
        LocalServerConfig conf = LocalServers.configFile(DIR);
        PatchStoreRocks patchStore = (PatchStoreRocks)PatchStoreMgr
                                            .getPatchStoreProvider(DPS.PatchStoreDatabaseProvider)
                                            .create(conf);
        patchStore.initialize(dataRegistry, conf);
        return patchStore;
    }
}
