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

package org.seaborne.delta;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.seaborne.delta.lib.LogX;

@RunWith(Suite.class)
@SuiteClasses( {
    TestLocalLinkMem.class ,
    TestLocalLinkFile.class ,
    TestLocalLinkRocksDB.class ,
    TestLocalLinkZk.class ,

    TestLocalConnectionMem.class ,
    TestLocalConnectionFile.class ,
    TestLocalConnectionZk.class ,
    TestLocalConnectionRocksDB.class ,
    TestLocalClient.class ,

    TestRemoteLink.class ,
    TestRemoteConnection.class ,
    TestRemoteClient.class ,

    TestZone.class ,
    TestRestart.class ,

    TestManagedDatasetBuilder.class,
    TestManagedDatasetBuilder2.class,
    TestDeltaAssembler.class,

    TestLogLockMem.class,
    TestLogLockFile.class,
    TestLogLockZk.class,
    TestDeltaLogLockMem.class,
    TestDeltaLogLockFile.class,
    TestDeltaLogLockZk.class,

    // Includes assembler tests.
    TestDeltaFusekiGood.class ,
    TestDeltaFusekiBad.class ,

    TestReleaseSetup.class
})

public class TS_Delta {
    @BeforeClass public static void setForTesting() {
        LogX.setJavaLogging("src/test/resources/logging.properties");
    }
}
