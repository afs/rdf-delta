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

package org.seaborne.delta.server.local.patchstores.mem;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.ext.com.google.common.collect.Lists;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchLog;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchStoreProvider;
import org.seaborne.delta.server.local.patchstores.PatchLogBase;

public class PatchStoreMem extends PatchStore {
    private Map<DataSourceDescription, PatchLog> logs = new ConcurrentHashMap<>();
    
    public PatchStoreMem(PatchStoreProvider provider) {
        super(provider);
    }

    @Override
    protected PatchLog create(DataSourceDescription dsd) {
        PatchLog plog = new PatchLogBase(dsd, new PatchLogIndexMem(), new PatchStorageMem(), this);
        logs.put(dsd, plog);
        return plog;
    }

    @Override
    protected void delete(PatchLog patchLog) {
        logs.remove(patchLog.getDescription());
    }

    @Override
    public List<DataSourceDescription> listDataSources() {
        return Lists.newArrayList(logs.keySet());
    }

    @Override
    protected List<DataSourceDescription> initialize(LocalServerConfig config) { 
        return Collections.emptyList();
    }

    @Override
    protected void releaseStore() {
        logs.clear();
    }

    @Override
    protected void deleteStore() {
        releaseStore();
    }
}
