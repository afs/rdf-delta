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

import java.util.*;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.ext.com.google.common.collect.BiMap ;
import org.apache.jena.ext.com.google.common.collect.HashBiMap ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Known {@link PatchStore}s. A {@link PatchStore} manages a number of {@link PatchLog}s.
 * <p>
 * There is a default {@link PatchStore} where new patch logs are created unless
 * otherwise specificed.
 */
public class PatchStoreMgr {
    protected static Logger LOG = LoggerFactory.getLogger(PatchStoreMgr.class); 
    
    private static Map<String, PatchStoreProvider> providers = new HashMap<>();
    
    // Default PatchStore.
    private static PatchStoreProvider dxftPatchStoreProvider;

    // ---- Short name / long name.
    private static BiMap<String, String> shortName2LongName = HashBiMap.create();
    
    public static void registerShortName(String shortName, String providerName) {
        shortName2LongName.put(shortName, providerName);
    }

    /** Short name to full provider name.
     *  A return of null means "don't know".
     */
    public static String shortName2LongName(String shortName) {
        if ( shortName == null )
            return null;
        return shortName2LongName.get(shortName);
    }
    
    public static String longName2ShortName(String providerName) {
        return shortName2LongName.inverse().get(providerName);
    }
    // ----
    
    public static Collection<PatchStoreProvider> registered() {
        return new HashSet<>(providers.values());
    }

    public static boolean isRegistered(String providerName) {
        return providers.containsKey(providerName);
    }
    
    /**
     * Add a {@link PatchStore} using the given {@link PatchStoreProvider} for details and
     * to create the {@code PatchStore}
     */
    public static void register(PatchStoreProvider provider) {
      registerShortName(provider.getShortName(), provider.getProviderName());
      providers.put(provider.getProviderName(), provider);
    }
    
    /** Unregister by provider name */ 
    public static void unregister(String providerName) {
        FmtLog.info(LOG, "Unregister patch store: %s", providerName);
        if ( ! providers.containsKey(providerName) )
            Log.warn(PatchStore.class, "Not registered: "+providerName);
        PatchStoreProvider psp = providers.remove(providerName);
    }
    
    /**
     * Get the {@code PatchStoreProvider} by name.
     */
    public static PatchStoreProvider getPatchStoreProvider(String providerName) {
        String name = key(providerName);
        return providers.get(name); 
    }
    
    /** Return the preferred name */
    public static String canonical(String name) {
        return key(name);
    }
        
    private static String key(String name) {
        if ( isRegistered(name) )
            return name;
        name = shortName2LongName(name);
        if ( isRegistered(name) )
            return name;
        return null;
    }

    public static void reset() {
        shortName2LongName.clear();
        providers.clear();
    }
}
