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

import static org.seaborne.delta.server.local.patchstores.zk.Zk.zkPath;

import org.apache.curator.framework.CuratorFramework;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.server.local.PatchLog;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.patchstores.PatchLogBase;

/**
 * Zookeeper based {@link PatchLog}.
 * <p>
 * Zookeeper is not designed for large objects so patches may need to be keep elsewhere.
 * This class provides integrated use of Zookeeper with the practical limitations of
 * individual patch size.
 */
public class PatchLogZk extends PatchLogBase {
    // The decision of where to place it in the Zookeeper namespace is the responsibility of calling PatchStore.
    public PatchLogZk(DataSourceDescription dsd, String logPath, CuratorFramework client, PatchStore patchStore) {
        super(dsd,
              new PatchLogIndexZk(client, zkPath(logPath, ZkConst.nState), zkPath(logPath, ZkConst.nVersions)),
              new PatchStorageZk(client, zkPath(logPath, ZkConst.nPatches)),
              patchStore);
    }
}
