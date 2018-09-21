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

package org.seaborne.delta.server.s3;

import org.apache.curator.framework.CuratorFramework;
import org.seaborne.delta.server.local.PatchStoreProvider;
import org.seaborne.delta.server.local.patchstores.zk.PatchStoreZk;
import org.seaborne.delta.server.s3.PatchStoreProviderZkS3.DetailsS3;

public class PatchStoreZkS3 extends PatchStoreZk {

    private final DetailsS3 s3;

    PatchStoreZkS3(CuratorFramework client, PatchStoreProvider psp, DetailsS3 s3) {
        super(client, psp);
        this.s3 = s3;
    }

    public DetailsS3 access() { return s3; }
}
