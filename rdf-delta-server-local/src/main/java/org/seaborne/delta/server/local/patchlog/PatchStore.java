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

package org.seaborne.delta.server.local.patchlog;

import java.nio.file.Path ;
import java.util.HashMap ;
import java.util.Map ;
import java.util.concurrent.ConcurrentHashMap ;

import org.apache.jena.atlas.logging.FmtLog ;
import org.apache.jena.atlas.logging.Log ;
import org.seaborne.delta.DeltaConfigException ;
import org.seaborne.delta.DeltaException ;
import org.seaborne.delta.Id ;
import org.seaborne.delta.lib.IOX ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public abstract class PatchStore {
    // Needs revisiting to redesign.
    // XXX shared default setting assumes only one LocalServer
    
    protected static Logger LOG = LoggerFactory.getLogger(PatchStore.class); 
    
    // -------- PatchStore.Provider
    // Providers wire themselves in during server startup.
    // Providers should not be be removed if there are any in use. 
    static Map<String, PatchStore> patchStores = new HashMap<>();

    public static boolean isRegistered(String providerName) {
        return patchStores.containsKey(providerName);
    }
    
    // The provider name is used in config files. 
    public static void register(PatchStore impl) {
        String providerName = impl.getProviderName();
        FmtLog.info(LOG, "register patch store: %s", providerName);
        if ( patchStores.containsKey(providerName) )
            Log.error(PatchStore.class, "Already registered: "+providerName);
        patchStores.put(providerName, impl);
    }
    
    public static void setDefault(String provideName) {
        PatchStore impl = patchStores.get(provideName);
        if ( impl == null )
            throw new DeltaConfigException("No provider for '"+provideName+"'");  
        dftPatchStore = impl;
    }
    
    public static String getDefault() {
        if ( dftPatchStore == null )
            return null;
        return dftPatchStore.getProviderName();
    }
    
    // ---- PatchStore.Provider

    // -------- Global
    // XXX Split out PatchStore.Provider management.
    private static Map<Id, PatchLog> logs = new ConcurrentHashMap<>();
    private static PatchStore dftPatchStore;

    /**
     * Get the {@link PatchStore}. Return the current global default if not
     * specifically found
     */
    public static PatchStore selectPatchStore(Id dsRef) {
        // Look in existing bindings.
        PatchStore patchStore = patchStores.get(dsRef);
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
    
    /**
     * Get the PatchStore by provider name.
     */
    public static PatchStore getPatchStoreByProvider(String providerName) {
        return patchStores.get(providerName);
    }
    

    /** Return the {@link PatchLog}, which must already exist. */ 
    public static PatchLog getLog(Id dsRef) { 
        return logs.get(dsRef);
    }
    
    protected static boolean logExists(Id dsRef) {
        return logs.containsKey(dsRef);
    }
    
    /**
     * Release ("delete") the {@link PatchLog}. 
     * @param patchLog
     */
    public static void release(PatchLog patchLog) {
        Id dsRef = patchLog.getDescription().getDataSourceId();
        if ( ! logExists(dsRef) ) {
            FmtLog.warn(LOG, "PatchLog not known to PatchStore: dsRef=%s", dsRef);
            return;
        }
        logs.remove(dsRef);
        patchLog.release();
    }
    
    public static void clearPatchLogs() {
        logs.clear() ;
    }
    
    // ---- Global
    
    // -------- Instance
    private final String providerName ;
    
    protected PatchStore(String providerName) {
        this.providerName = providerName ;
    }
    
    /** Return the name of the provider implementation. */ 
    public String getProviderName() { 
        return providerName;
    }

    /**
     * Return a new {@link PatchLog}. Checking that there is no registered
     * {@link PatchLog} for this {@code dsRef} has already been done.
     * 
     * @param dsRef
     * @param dsName
     * @param path : Path to the Logs/ directory. Contents are PatchStore-impl specific.
     * @return PatchLog
     */
    protected abstract PatchLog create(Id dsRef, String dsName, Path path);
   
    /** Return a new {@link PatchLog}, which must not already exist. */ 
    public PatchLog createLog(Id dsRef, String dsName, Path path) { 
        // Path to "Log/" area workspace
        if ( logExists(dsRef) )
            throw new DeltaException("Can't create - PatchLog exists");
        IOX.ensureDirectory(path);
        PatchLog patchLog = create(dsRef, dsName, path);
        logs.put(dsRef, patchLog);
        return patchLog;
    }
}
