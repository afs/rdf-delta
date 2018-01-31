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

import org.seaborne.delta.Delta ;
import org.seaborne.delta.server.local.patchlog.PatchStore ;
import org.seaborne.delta.server.local.patchlog.PatchStoreFile ;
import org.seaborne.delta.server.local.patchlog.PatchStoreMem ;
import org.seaborne.delta.server.local.patchlog.PatchStoreMgr ;
import org.slf4j.Logger ;

public class DPS {
    
    public static Logger LOG = Delta.DELTA_LOG ;
    public static Logger HTTP_LOG = Delta.DELTA_HTTP_LOG ;
    
    public static String PatchStoreFileProvider = "PatchStoreFileProvider";
    public static String PatchStoreMemProvider = "PatchStoreMemProvider";
    
    private static volatile boolean initialized = false ;
    
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
        // First - because this may initial the system (tests called in isolation).
        LocalServer.releaseAll();
        PatchStoreMgr.reset();
        PatchStore.clearLogIdCache();
        // Init.
        DPS.initOnce();
        // This would be called after initialization, when LocalServer is first touched.
        LocalServer.initSystem();
    }
    
    // Things to do once.
    private static void initOnce() {
        PatchStoreFile.registerPatchStoreFile();
        PatchStoreMem.registerPatchStoreMem();
//        PatchStoreMgr.setDftPatchStoreName(DPS.PatchStoreMemProvider);
    }
}
