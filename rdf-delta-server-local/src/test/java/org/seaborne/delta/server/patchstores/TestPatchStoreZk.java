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

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.seaborne.delta.server.ZkT;
import org.seaborne.delta.server.local.*;

public class TestPatchStoreZk extends AbstractTestPatchStore {
    
    @Before public void beforeZkTest() {} 
    @After public void afterZkTest() { ZkT.clearAll(); }
    
    
    @Override
    protected PatchStore patchStore(DataRegistry dataRegistry) {
        TestingServer server = ZkT.localServer();
        String connectionString = server.getConnectString();
        LocalServerConfig config = LocalServers.configZk(connectionString);
        PatchStore patchStore = PatchStoreMgr.getPatchStoreProvider(DPS.PatchStoreZkProvider).create(config);
        patchStore.initialize(dataRegistry, config);
        return patchStore;
    }
}
