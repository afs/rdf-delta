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

import org.apache.jena.atlas.logging.Log;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.server.Provider;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchStoreProvider;

/**
 * A patch store provider that can rebuild from any local (file-based, or RockDB-based) installation.
 * This allows for a mixed storage implementation.
 */

public class PatchStoreProviderAny implements PatchStoreProvider {

    public PatchStoreProviderAny() {}

    @Override
    public PatchStore create(LocalServerConfig config) {
        // The directory where all patch logs are kept.
        String patchLogDirectory = config.getProperty(DeltaConst.pDeltaStore);
        if ( patchLogDirectory == null ) {
            Log.error(this, "No file area setting in the configuration for local patch storage setup");
            throw new DeltaConfigException("No file area setting in the configuration for local patch storage setup");
        }
        return create(patchLogDirectory);
    }

    public PatchStoreAnyLocal create(String patchLogDirectory) {
        if ( patchLogDirectory == null )
            return null;
        return new PatchStoreAnyLocal(patchLogDirectory, this);
    }

    @Override
    public Provider getProvider() { throw new DeltaException("PatchStoreProviderAny.getProvider() called"); }

    @Override
    public String getShortName() {
        return "AnyLocal";
    }
}
