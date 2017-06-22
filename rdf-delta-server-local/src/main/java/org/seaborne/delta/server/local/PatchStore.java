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

package org.seaborne.delta.server.local;

import java.util.Map ;
import java.util.concurrent.ConcurrentHashMap ;

import org.seaborne.delta.DeltaException ;
import org.seaborne.delta.Id ;

public class PatchStore {
    
    //DataPatchServer:

    // Need to reverse DataSource.getPatchLog() - indirection to allow switch PatchStores.
    
    // And factory
    
    // Store = collection logs.
    
    // -- Global
    private static Map<Id, PatchStore> known = new ConcurrentHashMap<>();
    private static PatchStore dftPatchStore = new PatchStore();
    
    // -- Instance
    // DataSource 
    private Map<Id, PatchLog> logs = new ConcurrentHashMap<>();
    
    private PatchStore() {}
    
    /** Get the PatchStore */
    public static PatchStore getPatchStore(Id dsRef) {
        // Look in existing bindings.
        PatchStore patchStore = known.get(dsRef);
        if ( patchStore != null )
            return patchStore;
        return getPatchStore();
    }
    
    /**
     * Get the current default {@code PatchStore}, e.g. for creating new {@link PatchLog}s.
     */
    public static PatchStore getPatchStore() {
        return dftPatchStore ;
    }

    public PatchLog getLog(Id dsRef) { 
        return getPatchStore(dsRef).logs.get(dsRef);
    }
    
    private boolean logExists(Id dsRef) {
        return getPatchStore(dsRef).logs.containsKey(dsRef);
    }

    // The log for the {@link DataSource} (there is only one for each DataSource 
    public PatchLog createLog(Id dsRef) { 
        if ( logExists(dsRef) )
            throw new DeltaException("PatchLog exists");
        return null;
    }
    
    public PatchLog deleteLog(Id dsRef) { return null; }
    
    int szie() { return 0 ; }
    
}
