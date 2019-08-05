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

package org.seaborne.delta.server;

import org.apache.jena.atlas.logging.LogCtl;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.seaborne.delta.server.patchstores.*;

@RunWith(Suite.class)
@Suite.SuiteClasses( {
    TestLocalServerBuildConfig.class

    , TestPatchStorageMem.class
    , TestPatchStorageZk.class
    , TestFileStore.class

    , TestPatchLogIndexMem.class
    // XXX ToDo
    //, TestPatchLogIndexFile.class
    //, TestPatchLogIndexZk.class

    , TestPatchLogMem.class
    , TestPatchLogFile1.class   // Original
    , TestPatchLogFile2.class   // New
    , TestPatchLogZk.class

    , TestPatchStoreMem.class
    , TestPatchStoreFile.class
    , TestPatchStoreZk.class

    , TestLocalServer.class
    , TestLocalServerCreateDelete.class
})

public class TS_ServerLocal {
    @BeforeClass public static void beforeClass() {
        LogCtl.setJavaLogging("src/test/resources/logging.properties");
    }
}


