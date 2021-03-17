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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.seaborne.delta.server.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Known {@link PatchStore}s. A {@link PatchStore} manages a number of {@link PatchLog}s.
 * <p>
 * There is a default {@link PatchStore} where new patch logs are created unless
 * otherwise specified.
 */
public class PatchStoreMgr {
    protected static Logger LOG = LoggerFactory.getLogger(PatchStoreMgr.class);

    private static Map<Provider, PatchStoreProvider> providers = new HashMap<>();


    /**
     * Add a {@link PatchStore} using the given {@link PatchStoreProvider} for details and
     * to create the {@code PatchStore}
     */
    public static void register(PatchStoreProvider patchStoreProvider) {
        Provider provider = patchStoreProvider.getType();
        DPS.registerShortName(patchStoreProvider.getShortName(), provider);
        providers.put(provider, patchStoreProvider);
    }

    /** Unregister by provider name */
    public static void unregister(Provider provider) {
        FmtLog.info(LOG, "Unregister patch store: %s", provider);
        if ( ! providers.containsKey(provider) )
            Log.warn(PatchStore.class, "Not registered: "+provider);
        PatchStoreProvider psp = providers.remove(provider);
    }

    // ----

    public static Collection<PatchStoreProvider> registered() {
        return new HashSet<>(providers.values());
    }

    public static boolean isRegistered(Provider provider) {
        return providers.containsKey(provider);
    }

    /**
     * Get the {@link PatchStoreProvider} by enum-name.
     */
    public static PatchStoreProvider getPatchStoreProvider(Provider provider) {
        return providers.get(provider);
    }

    public static void reset() {
        providers.clear();
    }
}
