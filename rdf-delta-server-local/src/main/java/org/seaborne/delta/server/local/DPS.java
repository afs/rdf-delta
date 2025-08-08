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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.seaborne.delta.Delta;
import org.seaborne.delta.server.Provider;
import org.seaborne.delta.server.local.patchstores.any.PatchStoreProviderAnyLocal;
import org.seaborne.delta.server.local.patchstores.file.PatchStoreFile;
import org.seaborne.delta.server.local.patchstores.file.PatchStoreProviderFile;
import org.seaborne.delta.server.local.patchstores.filestore.FileStore;
import org.seaborne.delta.server.local.patchstores.mem.PatchStoreProviderMem;
import org.seaborne.delta.server.local.patchstores.rdb.PatchStoreProviderRocks;
import org.seaborne.delta.server.local.patchstores.rdb.PatchStoreRocks;
import org.seaborne.delta.server.local.patchstores.zk.PatchStoreProviderZk;
import org.seaborne.delta.server.system.DeltaSystem;
import org.slf4j.Logger;

/** Delta Patch Server */
public class DPS {

    public static Logger LOG = Delta.DELTA_LOG;
    public static Logger HTTP_LOG = Delta.DELTA_HTTP_LOG;

    private static volatile boolean initializedFirst = false;
    private static volatile boolean initializedLast = false;

    // Constants for PatchStoreProvider names.
    public static final String pspFile    = "file";
    public static final String pspRocks   = "rdb";
    public static final String pspMem     = "mem";
    public static final String pspZk      = "zk";
    public static final String pspLocal   = "local";

    // ---- Provider name registry.
    private static Map<String, Provider> providerByName = new ConcurrentHashMap<>();

    /** Register provided by name (name used in log_type). */
    public static void registerShortName(String name, Provider provider) {
        String cname = canonicalProviderName(name);
        providerByName.put(cname, provider);
    }

    /**
     * Return the {@link Provider} registered with the given name.
     * A return of null means "no such provider registered".
     */
    public static Provider providerByName(String name) {
        if ( name == null )
            return null;
        String cname = canonicalProviderName(name);
        return providerByName.get(cname);
    }
    // ----

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
        initFirst();
        initLast();
    }

    /**
     * For testing. This code knows where all the global state is
     * and reset the system to the default after init() called.
     * The default PatchStoreProviders are retained.
     */
    public static void resetSystem() {
        DeltaSystem.init();
        initFirst();

        // Clear any state.
        LocalServer.releaseAll();
        FileStore.resetTracked();
        PatchStoreFile.resetTracked();
        PatchStoreRocks.resetTracked();

        providerByName.clear();
        PatchStoreMgr.reset();

        initPatchStoreProviders();

        initLast();
    }

    // Things to do once per initialization
    private static void initPatchStoreProviders() {
        // The standard set of PatchStoreProviders
        List<PatchStoreProvider> providers = Arrays.asList(
            new PatchStoreProviderFile(),
            new PatchStoreProviderRocks(),
            new PatchStoreProviderMem(),
            new PatchStoreProviderZk(),
            new PatchStoreProviderAnyLocal()
        );

        providers.forEach(psp->{
            LOG.debug("Provider: "+psp.getShortName());
            PatchStoreMgr.register(psp);
        });
    }

    private static String canonicalProviderName(String name) {
        if ( name== null )
            return null;
        return name.toLowerCase(Locale.ROOT).trim();
    }
}
