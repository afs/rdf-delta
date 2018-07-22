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
import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.Id;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.delta.zk.Zk;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Patch storage in Apache ZooKeeper. 
 * <p>
 * <b>Note</b> Apache ZooKeeper is <a href="https://zookeeper.apache.org/doc/r3.4.12/zookeeperProgrammers.html#Data+Access"
 * >not designed for storing large objects</a>. 
 * The default maximum is 1M and most data for znodes should be much less that that. 
 * They can cause slow startup because ZooKeeper keeps the database in-memory.
 */
public class PatchStorageZk implements PatchStorage {
    private static Logger LOG = LoggerFactory.getLogger(PatchStorageZk.class); 
    private final CuratorFramework client;
    private final String patches;

    public PatchStorageZk(CuratorFramework client, int instance, String logPath) {
        this.client = client;
        this.patches = Zk.zkPath(logPath, ZkConst.nPatches);
        Zk.zkEnsure(client, patches);
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
            FmtLog.warn(LOG, "fetch(%s) : Zero bytes", key);
        return RDFPatchOps.read(new ByteArrayInputStream(b));
    }

    @Override
    public void delete(Id id) { 
        String p = Zk.zkPath(patches, id.asPlainString());    
        Zk.zkRun(()->client.delete().forPath(p));
    }

    
    @Override
    public void release() { 
        find().forEach(this::delete);
    }
}
