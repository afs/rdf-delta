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

import java.nio.file.Paths;

import org.apache.jena.atlas.lib.FileOps;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.delta.server.local.patchstores.rdb.PatchStorageRocks;
import org.seaborne.delta.server.local.patchstores.rdb.RocksDatabase;

public class TestPatchStorageRocks extends AbstractTestPatchStorage {
    private static String DIR = "target/test/patch-store-file/db";

    @BeforeClass public static void beforeClass() {
        FileOps.ensureDir(DIR);
        FileOps.clearAll(DIR);
    }

    private RocksDatabase rdb = null;
    @Before public void before() {
        rdb = new RocksDatabase(Paths.get(DIR));
    }

    @After public void after() {
        rdb.close();
        FileOps.clearAll(DIR);
    }

    @Override
    protected PatchStorage patchStorage() {
        return new PatchStorageRocks(rdb);
    }
}
