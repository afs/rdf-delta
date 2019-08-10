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

package org.seaborne.delta.server.local;

import java.util.ArrayList;
import java.util.List;

import org.seaborne.delta.Delta;
import org.seaborne.delta.server.local.patchstores.file2.PatchStoreFile;
import org.seaborne.delta.server.local.patchstores.file2.PatchStoreProviderFile;
import org.seaborne.delta.server.local.patchstores.filestore.FileStore;
import org.seaborne.delta.server.local.patchstores.mem.PatchStoreProviderMem;
import org.seaborne.delta.server.local.patchstores.rdb.PatchStoreProviderRocks;
import org.seaborne.delta.server.local.patchstores.rdb.PatchStoreRocks;
import org.seaborne.delta.server.local.patchstores.zk.PatchStoreProviderZk;
import org.seaborne.delta.server.system.DeltaSystem;
import org.slf4j.Logger;

public class DPS {

    public static Logger LOG = Delta.DELTA_LOG;
    public static Logger HTTP_LOG = Delta.DELTA_HTTP_LOG;

    private static volatile boolean initializedFirst = false;
    private static volatile boolean initializedLast = false;

//    public static String PatchStoreFileProvider     = "PatchStore/File";
//    public static String PatchStoreDatabaseProvider = "PatchStore/DB";
//    public static String PatchStoreMemProvider      = "PatchStore/Mem";
//    public static String PatchStoreZkProvider       = "PatchStore/Zk";

    // Short names.
    public static final String pspFile    = "file";
    public static final String pspRocks   = "db";
    public static final String pspMem     = "mem";
    public static final String pspZk      = "zk";
    public static final String pspZkS3      = "zks3";

    public static void initFirst() {
        if ( initializedFirst )
            return;
        synchronized(DPS.class) {
            if ( initializedFirst )
                return;
            initializedFirst = true;
            DeltaSystem.init();
        }
    }

    public static void initLast() {
        if ( initializedLast )
            return;
        synchronized(DPS.class) {
            if ( initializedLast )
                return;
            initializedLast = true;
            initPatchStoreProviders();
        }
    }

    public static void init() {
        DeltaSystem.init();
    }

    /**
     * For testing. This code knows where all the global state is
     * and reset the system to the default after init() called.
     * The default PatchStoreProviders are retained.
     */
    public static void resetSystem() {
        DeltaSystem.init();
        DPS.initFirst();

        // Clear any state.
        LocalServer.releaseAll();
        FileStore.resetTracked();
        PatchStoreFile.resetTracked();
        PatchStoreRocks.resetTracked();
        // PatchStoreMgr.reset clears the patch store providers
        PatchStoreMgr.reset();

        initPatchStoreProviders();
        // And ZkS3?

        DPS.initLast();
    }

    // Things to do once.
    private static void initPatchStoreProviders() {
        List<PatchStoreProvider> providers = new ArrayList<>();

        // Hard coded the discovery.
        providers.add(new PatchStoreProviderFile());
        //providers.add(new PatchStoreProviderFileOriginal());
        providers.add(new PatchStoreProviderRocks());
        providers.add(new PatchStoreProviderMem());
        providers.add(new PatchStoreProviderZk());

        providers.forEach(psp->{
            LOG.debug("Provider: "+psp.getProvider().toString().toLowerCase()+"{"+psp.getShortName()+"}");
            PatchStoreMgr.register(psp);
        });
    }
}
