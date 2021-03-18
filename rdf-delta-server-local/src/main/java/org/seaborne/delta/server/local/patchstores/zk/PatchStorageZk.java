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

package org.seaborne.delta.server.local.patchstores.zk;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.stream.Stream;

import org.apache.curator.utils.ZKPaths;
import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.Id;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.delta.zk.UncheckedZkConnection;
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
    private final UncheckedZkConnection client;
    private final String patches;

    public PatchStorageZk(UncheckedZkConnection client, String instance, String logPath) {
        this.client = client;
        this.patches = ZKPaths.makePath(logPath, ZkConst.nPatches, new String[]{});
        client.ensurePathExists(patches);
    }

    @Override
    public Stream<Id> find() {
        List<String> x = this.client.fetchChildren(patches);
        return x.stream().map(s-> Id.fromString(s));
    }

    @Override
    public void store(Id key, RDFPatch value) {
        String p = ZKPaths.makePath(patches, key.asPlainString(), new String[]{});
        ByteArrayOutputStream out = new ByteArrayOutputStream(10*1024);
        RDFPatchOps.write(out, value);
        byte[] b = out.toByteArray();
        this.client.createAndSetZNode(p, b);
    }

    @Override
    public RDFPatch fetch(Id key) {
        String p = ZKPaths.makePath(patches, key.asPlainString(), new String[]{});
        byte[] b = this.client.fetch(p);
        if ( b == null )
            return null;
        if ( b.length == 0 )
            FmtLog.warn(LOG, "fetch(%s) : Zero bytes", key);
        return RDFPatchOps.read(new ByteArrayInputStream(b));
    }

    @Override
    public void delete(Id id) {
        String p = ZKPaths.makePath(patches, id.asPlainString(), new String[]{});
        this.client.deleteZNodeAndChildren(p);
    }

    @Override
    public void release() {
        find().forEach(this::delete);
    }
}
