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

import com.amazonaws.services.s3.AmazonS3;

import org.junit.After;
import org.junit.Before;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.delta.server.patchstores.AbstractTestPatchStorage;
import org.seaborne.delta.server.s3.PatchStorageS3;

public class TestPatchStorageS3_Real extends AbstractTestPatchStorage {

    private AmazonS3 client;
    private String bucketName = "test-bucket";
    private String prefix = "delta/";

    @Before public void before() {
        client = S3T.makeMockS3();
    }
    
    @After public void after() {
        client.shutdown();
    }
            
    @Override
    protected PatchStorage patchStorage() {
        return new PatchStorageS3(client, bucketName, prefix);
    }
}
