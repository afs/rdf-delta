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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.atlas.lib.ListUtils;
import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.Id;
import org.seaborne.delta.server.local.patchstores.PatchLogBase;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code PatchStore} is manager for a number of {@link PatchLog}s. One {@link PatchLog}
 * is the log for one data source; a {@code PatchStore} is a collection of such logs and
 * it is responsible for initialization, restoring {@code PatchLog}s on external storage
 * (files etc.)
 * <p>
 * There is normally one {@code PatchStore} for each patch storage technology but this is
 * not a requirement.
 */
public abstract class PatchStore {
    private static Logger LOG = LoggerFactory.getLogger(PatchStore.class);

    // ---- PatchStore.Provider

    // -------- Global
    // DataRegistry? That is all of the LocalServer i.e. all patch stores.
    private Map<Id, PatchLog> logs = new ConcurrentHashMap<>();

    /** Return the {@link PatchLog}, which must already exist. */
    public PatchLog getLog(Id dsRef) {
        return logs.get(dsRef);
    }

    public boolean logExists(Id dsRef) {
        return logs.containsKey(dsRef);
    }

    /** Clear the internal mapping from Log (by Id) to its PatchLog. Used for testing. */
    public void clearLogIdCache() {
        logs.clear();
    }

    // ---- /Global

    // -------- Instance
    private final PatchStoreProvider provider;

    private DataRegistry dataRegistry = null;

    private LocalServerConfig configuration;

    protected PatchStore(PatchStoreProvider provider) {
        this.provider = provider;
    }

    /** Return the provider implementation. */
    public PatchStoreProvider getProvider() {
        return provider;
    }

    /** For subclasses of {@link PatchStore} to override - some don't need to do anything, Zookeeper ones do. */
    protected void sync() {}

    public DataRegistry getDataRegistry() {
        checkInitialized();
        return dataRegistry;
    }

    /**
     * Initialize a {@code PatchStore}.
     * <p>
     * The {@link DataRegistry} is used to route incoming requests,
     * by name the patch log name, to {@link PatchLog PatchLogs}; this argument may be null
     * for {@code PatchStores} not attached to a server (testing, development cases).
     * Only {@link DataSource DataSources} that are compatible with the {@code PatchStore} provider called
     * should be included in the returned list.
     */
    public List<DataSourceDescription> initialize(DataRegistry dataRegistry, LocalServerConfig config) {
        this.dataRegistry = dataRegistry;
        this.configuration = config;
        List<DataSourceDescription> descr = initialize(config);
        descr.forEach(dsd->createPatchLog(dsd));
        return descr;
    }

    private void checkInitialized() {
        if ( dataRegistry == null )
            throw new InternalErrorException("PatchStore not initialized");
    }

    /**
     * Initialize a patch store and provide a list of existing logs.
    */
    protected abstract List<DataSourceDescription> initialize(LocalServerConfig config);

    final
    public void shutdown() {
        releaseStore();
    }

    /** All the patch logs currently managed by this {@code PatchStore}. */
    public List<DataSourceDescription> listDataSources() {
        checkInitialized();
        return ListUtils.toList(dataRegistry.dataSources().map(log->log.getDescription()));
    }

    // XXX Implement getDataSource(String name) : use in PatchStoreZk.
    //public DataSourceDescription getDataSource(String name) { return null; }

    /**
     * Return a new {@link PatchLog}. Checking that there isn't a patch log for this
     * {@link DataSourceDescription} has already been done.
     *
     * @param dsd
     * @return PatchLog
     */
    protected abstract PatchLog newPatchLog(DataSourceDescription dsd);

    /**
     * Help to build a {@link PatchLog} from a {@link PatchLogIndex} and
     * {@link PatchStorage} by calling operation of the {@link PatchStoreProvider}.
     *
     * @param dsd
     * @return
     */
    protected PatchLog newPatchLogFromProvider(DataSourceDescription dsd) {
        PatchLogIndex patchLogIndex = getProvider().newPatchLogIndex(dsd, this, configuration);
        PatchStorage patchStorage = getProvider().newPatchStorage(dsd, this, configuration);
        return new PatchLogBase(dsd, patchLogIndex, patchStorage, this);
    }

    /** Return a new {@link PatchLog}, which must already exist and be registered. */
    public PatchLog connectLog(DataSourceDescription dsd) {
        FmtLog.info(LOG, "Connect log: %s", dsd);
        checkInitialized();
        PatchLog pLog = getLog(dsd.getId());
        if ( pLog == null )
            pLog = createPatchLog(dsd);
        return pLog;
    }

    /** Return a new {@link PatchLog}, which must not already exist. */
    public PatchLog createLog(DataSourceDescription dsd) {
        if ( getProvider() == null )
            FmtLog.info(LOG, "Create log[?]: %s", dsd);
        else
            FmtLog.info(LOG, "Create log[%s]: %s", getProvider().getShortName(), dsd);
        checkInitialized();
        Id dsRef = dsd.getId();
        if ( logExists(dsRef) )
            throw new DeltaException("Can't create - PatchLog exists");
        return createPatchLog(dsd);
    }

    /** Create and properly register a new {@link PatchLog}.
     *  Call this to add new patch logs including remote changes.
     *  This method calls {@link #create} provided by the subclass.
     *  This method called by PatchStoreZk when a new log appears.
     */
    final
    protected PatchLog createPatchLog(DataSourceDescription dsd) {
        Id dsRef = dsd.getId();
        PatchLog patchLog = newPatchLog(dsd);
        logs.put(dsRef, patchLog);
        if ( dataRegistry != null ) {
            DataSource dataSource = new DataSource(dsd, patchLog);
            // XXX Isn't this done in LocalServer.createDataSource$ as well?
            dataRegistry.add(dataSource);
        }
        return patchLog;
    }

    // XXX Sort out/verify the responsibility and call order for crate/release, all paths.
    // LocalServer/PatchStore on its own/Zk async update.
    // Single place to release, including call from LocalServer.

    /**
     * Release ("delete") the {@link PatchLog}.
     * Called from (1) LocalServer/client request and (2) releasePatchLog, cluster change.
     * This method calls {@link #delete} provided by the subclass.
     */
    public void release(PatchLog patchLog) {
        Id dsRef = patchLog.getLogId();
        // See also LocalServer.removeDataSource$
        if ( ! dataRegistry.contains(dsRef) ) {
            FmtLog.warn(LOG, "PatchLog not known to PatchStore: dsRef=%s", dsRef);
            return;
        }
        dataRegistry.remove(dsRef);
        logs.remove(dsRef);
        delete(patchLog);
    }

    /**
     * Remove and properly de-register a {@link PatchLog}.
     * Call this from subclasses notifying the deletion of a patch log elsewhere.
     */
    final
    protected void releasePatchLog(Id dsRef) {
        DataSource ds = dataRegistry.get(dsRef);
        if ( ds == null )
            return ;
        release(ds.getPatchLog());
    }

    /**
     * Delete a {@link PatchLog}.
     * @param patchLog
     */
    protected abstract void delete(PatchLog patchLog);

    /** Stop using this {@code PatchStore} - subclasses release resources. */
    protected abstract void releaseStore();

    /** Delete this {@code PatchStore}. */
    protected abstract void deleteStore();

}
