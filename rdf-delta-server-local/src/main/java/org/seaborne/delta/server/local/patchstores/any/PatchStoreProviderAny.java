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

package org.seaborne.delta.server.local.patchstores.any;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.server.Provider;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchStoreProvider;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.seaborne.delta.server.local.patchstores.PatchStorage;

/**
 * A patch store provider that can rebuild from any local (file-based, or RockDB-based) installation.
 * This allows for a mixed storage implementation.
 */

public class PatchStoreProviderAny implements PatchStoreProvider {

    @Override
    public PatchStore create(LocalServerConfig config) {
        // The directory where all patch logs are kept.
        String patchLogDirectory = config.getProperty(DeltaConst.pDeltaFile);
        return create(patchLogDirectory);
    }

    public PatchStore create(String patchLogDirectory) {
        // SPECIAL
        if ( patchLogDirectory == null )
            return null;
        return null;
        //return new PatchStoreRocks(patchLogDirectory, this);
    }

    @Override
    public Provider getProvider() { return null; }

    @Override
    public String getShortName() {
        return null;
    }

    @Override
    public PatchLogIndex newPatchLogIndex(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration) {
        return null;
    }

    @Override
    public PatchStorage newPatchStorage(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration) {
        return null;
    }

}
