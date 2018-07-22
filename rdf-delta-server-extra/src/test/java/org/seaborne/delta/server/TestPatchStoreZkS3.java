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

package org.seaborne.delta.server;

import org.junit.After;
import org.junit.Before;
import org.seaborne.delta.server.local.DataRegistry;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.patchstores.AbstractTestPatchStore;

public class TestPatchStoreZkS3 extends AbstractTestPatchStore {

    private PatchStore patchStore;

    @Before public void beforeZkS3() {
        patchStore = S3T.setup();
    }
    @After public void afterZkS3() {
        patchStore.shutdown();
        ZkT.clearAll();
    }
    
    @Override
    protected PatchStore patchStore(DataRegistry dataRegistry) {
        return patchStore;
    }

}
