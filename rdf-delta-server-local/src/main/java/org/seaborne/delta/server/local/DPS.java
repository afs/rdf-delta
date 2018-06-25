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

import java.util.ArrayList;
import java.util.List;

import org.seaborne.delta.Delta ;
import org.seaborne.delta.server.local.patchlog.FileStore;
import org.seaborne.delta.server.local.patchlog.PatchStoreMgr;
import org.seaborne.delta.server.local.patchstores.file.PatchStoreProviderFile;
import org.seaborne.delta.server.local.patchstores.mem.PatchStoreProviderMem;
import org.slf4j.Logger ;

public class DPS {
    
    public static Logger LOG = Delta.DELTA_LOG ;
    public static Logger HTTP_LOG = Delta.DELTA_HTTP_LOG ;
    
    private static volatile boolean initialized = false ;
    
    public static String PatchStoreFileProvider = "PatchStore/File";
    public static String PatchStoreMemProvider  = "PatchStore/Mem";
    public static String PatchStoreZkProvider  = "PatchStore/Zk";
    
    public static void init() { 
        if ( initialized ) 
            return ;
        synchronized(DPS.class) {
            if ( initialized ) 
                return ;
            initialized = true ;
            initOnce() ;
        }
    }

    /**
     * For testing. This code knows where all the global state is and reset the
     * system to the default after init() called.
     */
    public static void resetSystem() {
        // Clear
        // First - because this may initialize the system (tests called in isolation).
        LocalServer.releaseAll();
        FileStore.resetTracked();
        PatchStoreMgr.reset();
        PatchStore.clearLogIdCache();
        // This would be called after initialization, when LocalServer is first touched.
        LocalServer.initSystem();
    }
    
    // Things to do once.
    private static void initOnce() {
        // Find PatchStoreProviders.
        List<PatchStoreProvider> providers = new ArrayList<>();
        
        // Hard code the discovery for now.
        providers.add(new PatchStoreProviderFile());
        providers.add(new PatchStoreProviderMem());
        //providers.add(new PatchStoreProviderZk());
        
        providers.forEach(psp->{
            PatchStore pStore = psp.create();
            LOG.info("Provider: "+pStore.getProviderName());
            PatchStoreMgr.registerShortName(psp.getShortName(), pStore.getProviderName());
            PatchStoreMgr.register(pStore);
        });
        //PatchStoreMgr.setDftPatchStoreName(DPS.PatchStoreZkProvider);
        PatchStoreMgr.setDftPatchStoreName(DPS.PatchStoreFileProvider);
    }
}
