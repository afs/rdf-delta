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

package org.seaborne.delta.server.local.patchstores.zk;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.stream.Stream;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.seaborne.delta.Id;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;

public class PatchStorageZk implements PatchStorage {

    private final CuratorFramework client;
    private final String patches;
    private final SharedCount versionCounter;

    public PatchStorageZk(CuratorFramework client, String patches) {
        this.client = client;
        this.patches = patches;
        this.versionCounter = null;
    }
    
    @Override
    public Stream<Id> find() {
        List<String> x = Zk.zkSubNodes(client, patches);
        return x.stream().map(s-> Id.fromString(s));
    }

    @Override
    public void store(Id key, RDFPatch value) {
        String p = Zk.zkPath(patches, key.asPlainString());
        ByteArrayOutputStream out = new ByteArrayOutputStream(10*1024);
        RDFPatchOps.write(out, value);
        byte[] b = out.toByteArray();
        Zk.zkCreateSet(client, p, b);
    }

    @Override
    public RDFPatch fetch(Id key) {
        String p = Zk.zkPath(patches, key.asPlainString());
        byte[] b = Zk.zkFetch(client, p);
        if ( b == null )
            return null;
        if ( b.length == 0 )
            System.err.println("Zero bytes");
        return RDFPatchOps.read(new ByteArrayInputStream(b));
    }
}
