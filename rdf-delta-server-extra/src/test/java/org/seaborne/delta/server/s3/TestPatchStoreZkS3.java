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

package org.seaborne.delta.server.s3;

import io.findify.s3mock.S3Mock;
import org.apache.jena.atlas.lib.Pair;
import org.junit.After;
import org.junit.Before;
import org.seaborne.delta.server.ZkT;
import org.seaborne.delta.server.local.DataRegistry;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.patchstores.AbstractTestPatchStore;

public class TestPatchStoreZkS3 extends AbstractTestPatchStore {
    private PatchStore patchStore;
    private S3Mock s3Mock ;

    @Before public void beforeZkS3() {
        Pair<PatchStore, S3Mock> pair = S3T.setup();
        patchStore = pair.getLeft();
        s3Mock = pair.getRight();
    }

    @After public void afterZkS3() {
        patchStore.shutdown();
        if ( s3Mock != null )
            s3Mock.shutdown();
        ZkT.clearAll();
    }

    @Override
    protected PatchStore patchStore(DataRegistry dataRegistry) {
        return patchStore;
    }

}
