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

package org.seaborne.delta.server.local.patchstores.rdb;

import org.apache.jena.atlas.logging.Log;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.server.Provider;
import org.seaborne.delta.server.local.DPS;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchStoreProvider;

public class PatchStoreProviderRocks implements PatchStoreProvider {

    public PatchStoreProviderRocks() {}

    @Override
    public PatchStore create(LocalServerConfig config) {
        // The directory where all patch logs are kept.
        String patchLogDirectory = config.getProperty(DeltaConst.pDeltaStore);
        if ( patchLogDirectory == null ) {
            Log.error(this, "No file area setting in the configuration for RocksDB based patch storage setup");
            throw new DeltaConfigException("No file area setting in the configuration for RocksDB based patch storage setup");
        }
        return create(patchLogDirectory);
    }

    public PatchStoreRocks create(String patchLogDirectory) {
        if ( patchLogDirectory == null )
            return null;
        return new PatchStoreRocks(patchLogDirectory, this);
    }

    @Override
    public Provider getType() { return Provider.ROCKS; }

    @Override
    public String getShortName() {
        return DPS.pspRocks;
    }
}
