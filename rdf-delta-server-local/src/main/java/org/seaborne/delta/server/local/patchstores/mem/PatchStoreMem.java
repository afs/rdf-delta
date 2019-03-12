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

package org.seaborne.delta.server.local.patchstores.mem;

import java.util.Collections;
import java.util.List;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchLog;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchStoreProvider;

/**
 * In-memory, ephemeral {@link PatchStore}.
 * All the work is done in
 * {@link PatchLogIndexMem} and {@link PatchStorageMem}.
 */
public class PatchStoreMem extends PatchStore {

    public PatchStoreMem(PatchStoreProvider provider) {
        super(provider);
    }

    @Override
    protected void delete(PatchLog patchLog) {}

    @Override
    protected List<DataSourceDescription> initialize(LocalServerConfig config) {
        return Collections.emptyList();
    }

    @Override
    protected void startStore() {}

    @Override
    protected void closeStore() {}

    @Override
    protected void deleteStore() { closeStore(); }

    @Override
    protected PatchLog newPatchLog(DataSourceDescription dsd) {
        return newPatchLogFromProvider(dsd);
    }
}
