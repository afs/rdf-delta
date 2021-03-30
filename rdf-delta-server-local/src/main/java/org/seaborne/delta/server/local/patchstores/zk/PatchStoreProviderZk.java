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

import org.apache.curator.framework.CuratorFramework;
import org.apache.jena.atlas.logging.Log;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.server.Provider;
import org.seaborne.delta.server.local.DPS;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchStoreProvider;
import org.seaborne.delta.zk.Zk;

public class PatchStoreProviderZk implements PatchStoreProvider {

    public PatchStoreProviderZk() { }

    @Override
    public PatchStore create(LocalServerConfig config) {
        CuratorFramework client = curator(config);
        return new PatchStoreZk(client, this, config.getProperty(ZkConst.prRootDirName));
    }

    /** Build a {@link CuratorFramework} from the {@link LocalServerConfig}. */
    protected CuratorFramework curator(LocalServerConfig config) {
        String connectionString = config.getProperty(DeltaConst.pDeltaZk);
        if ( connectionString == null )
            Log.error(this, "No connection string in configuration");
        CuratorFramework client = Zk.curator(connectionString);
        return client;
    }

    @Override
    public Provider getType() { return Provider.ZKZK; }

    @Override
    public String getShortName() {
        return DPS.pspZk;
    }
}
