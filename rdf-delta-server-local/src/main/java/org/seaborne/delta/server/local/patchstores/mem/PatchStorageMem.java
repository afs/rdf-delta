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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.seaborne.delta.Id;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.patch.RDFPatch;

/**
 * Patch store in-memory, nothing persisted. 
 */
public class PatchStorageMem implements PatchStorage {

    private Map<Id, RDFPatch> store = new LinkedHashMap<>();
    
    public PatchStorageMem() {}
    
    @Override
    public Stream<Id> find() {
        return store.keySet().stream();
    }

    @Override
    public void store(Id key, RDFPatch value) {
        store.put(key, value);
    }

    @Override
    public RDFPatch fetch(Id key) {
        return store.get(key);
    }

    @Override
    public void delete(Id id) {
        store.remove(id);
    }

//    @Override
//    public void release() {
//        store.clear();
//    }
}
