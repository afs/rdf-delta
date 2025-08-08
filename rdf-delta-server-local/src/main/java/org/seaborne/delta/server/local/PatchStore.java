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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.atlas.lib.DateTimeUtils;
import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.*;
import org.seaborne.delta.server.local.patchstores.PatchLogBase;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code PatchStore} is manager for a number of {@link PatchLog}s. One {@link PatchLog}
 * is the log for one data source; a {@code PatchStore} is a collection of such logs and
 * it is responsible for initialization, creation and deletion of {@code PatchLog}.
 * <p>
 * There is normally one {@code PatchStore} for each patch storage technology but this is
 * not a requirement.
 * <p>
 * {@code PatchStore} must not overlap - each {@link PatchLog} is controlled by exactly one {@code PatchStore}.
 */
public abstract class PatchStore {
    private static Logger LOG = LoggerFactory.getLogger(PatchStore.class);

    // --- Store-wide.
    // The logs managed by this PatchStore.
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

    private final PatchStoreProvider provider;

    // Global DataSourceRegistry from the LocalServer.
    // Injected in initialize.
    // Also used to test that initialization happened.
    private DataSourceRegistry dataSourceRegistry = null;

    private LocalServerConfig configuration;

    private boolean initialized = false;

    protected PatchStore(PatchStoreProvider provider) {
        this.provider = provider;
    }

    /** Return the provider implementation. */
    public PatchStoreProvider getProvider() {
        return provider;
    }

    /** For subclasses of {@link PatchStore} to override - some don't need to do anything. */
    protected void sync() {}

    public DataSourceRegistry getDataSourceRegistry() {
        return dataSourceRegistry;
    }

    /**
     * Initialize a {@code PatchStore}.
     * <p>
     * The {@link DataSourceRegistry} is used to route incoming requests, by mapping the
     * patch log name to {@link PatchLog PatchLogs}. Only {@link DataSource DataSources}
     * that are compatible with the {@code PatchStore} provider called should be included
     * in the returned list.
     * <p>
     * {@code initialize} is called exactly once.
     */
    public void initialize(DataSourceRegistry dataSourceRegistry, LocalServerConfig config) {
        // Initialization of LocalServer and PatchStore can't proceed in a simple nested
        // fashion. Significant work is needed for a PatchStore(rebuild from disk) so delaying
        // until the server starts is better.
        setDataSourceRegistry(dataSourceRegistry);
        this.configuration = config;
        initialize(config);
        markInitialized();
    }

    protected abstract void initialize(LocalServerConfig config);

    /** List the persistently stored patch logs of this {@code PatchStore},
     * by going to the actual storage.
     * <p>
     * Contrast this with {@link #listDataSources} which is expected to be fast and current.
     */
    protected abstract List<DataSourceDescription> initialDataSources();

    protected void serverStarts() {}

    /** Initialize a patch store and provide a list of existing logs. */
    private void setDataSourceRegistry(DataSourceRegistry dataSourceRegistry) {
        if ( this.dataSourceRegistry != null )
            throw new DeltaConfigException("Attempt to set patch store DataSourceRegistry more than once");
        this.dataSourceRegistry = dataSourceRegistry;
    }

    /** Create the initial patchlogs for this patchstore. */
    protected void createPatchLogs(List<DataSourceDescription> descr) {
        // createPatchLog also registers it.
        descr.forEach(dsd->createPatchLog(dsd));
    }

    /**
     * Mark this {@code PatchStore} as initialized. Normally, PatchStore implementation do
     * not need to call this. It is only needed for special cases like the "any local"
     * indirection than modifies the initialization sequence that may need to explicitly
     * indicate that initialization has been done. Initialization can only be done once.
     */
    private void markInitialized() {
        if ( initialized )
            throw new InternalErrorException("PatchStore already initialized");
        initialized = true;
        if ( dataSourceRegistry == null )
            throw new InternalErrorException("No DataSourceRegistry registry set during initialization phase");
    }

    private void checkInitialized() {
        if ( ! initialized )
            throw new InternalErrorException("PatchStore not initialized");
    }

    final
    public void shutdown() {
        shutdownSub();
        // Reset state?
    }

    protected abstract void shutdownSub();

    /** All the patch logs currently managed by this {@code PatchStore}. */
    public List<DataSourceDescription> listDataSources() {
        checkInitialized();
        return dataSourceRegistry.dataSources().map(log->log.getDescription()).toList();
    }

    /**
     * Return a new {@link PatchLog}. Checking that there isn't a patch log for this
     * {@link DataSourceDescription} has already been done. If so, return the existing one.
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
    protected PatchLog newPatchLogFromIndexAndStorage(DataSourceDescription dsd) {
        PatchLogIndex patchLogIndex = newPatchLogIndex(dsd, this, configuration);
        PatchStorage patchStorage = newPatchStorage(dsd, this, configuration);
        return new PatchLogBase(dsd, patchLogIndex, patchStorage, this);
    }

    /** Create a new {@link PatchLogIndex} for the given {@link DataSourceDescription}.
     * Part of the {@link #newPatchLogFromIndexAndStorage} cycle.
     */
    protected abstract PatchLogIndex newPatchLogIndex(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration2);

    /** Create a new {@link PatchStorage} for the given {@link DataSourceDescription}.
     * Part of the {@link #newPatchLogFromIndexAndStorage} cycle.
     */
    protected abstract PatchStorage newPatchStorage(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration2);

    /** Return a {@link PatchLog}, which must already exist and be registered. */
    public PatchLog connectLog(DataSourceDescription dsd) {
        FmtLog.info(LOG, "Connect log: %s", dsd);
        checkInitialized();
        PatchLog pLog = getLog(dsd.getId());
        if ( pLog == null )
            pLog = createPatchLog(dsd);
        return pLog;
    }

    private static Object lock = new Object();
    /** Return a new {@link PatchLog}, which must not already exist. */
    public PatchLog createLog(DataSourceDescription dsd) {
        synchronized(lock) {
            checkInitialized();
            Id dsRef = dsd.getId();
            sync();
            if ( logExists(dsRef) ) {
                PatchLog plog = logs.get(dsRef);
                FmtLog.debug(LOG, "Connect (%s): %s ", plog.getPatchStore().getProvider().getShortName(), dsd);
                return plog;
                //throw new DeltaException("Can't create - PatchLog exists");
            }

            if ( dataSourceRegistry.containsName(dsd.getName()) ) {
                DataSource ds = dataSourceRegistry.getByName(dsd.getName());
                PatchLog plog = ds.getPatchLog();
                FmtLog.debug(LOG, "Connect [name exists](%s): %s ",
                    plog.getPatchStore().getProvider().getShortName(),
                    dsd);

                return plog;
            }

            PatchLog plog = createPatchLog(dsd);
            if ( ! logExists(dsRef) ) {
                FmtLog.debug(LOG, "Create (invisible)");
            }

            FmtLog.debug(LOG, "Create (%s)", plog.getPatchStore().getProvider().getShortName());
            return plog;
        }
    }

    /*package*/ PatchLog rename(PatchLog patchLog, String oldName, String newName) {
        // This is "overlocking" - we're inside the LocalServer lock.
        // But this operation is not performance critical and not commonly used.
        // Maybe in the future there will be calls from something other that LocalServer.
        synchronized(lock) {
            if ( ! dataSourceRegistry.containsName(oldName) ) {
                // This may happen when operations are retried.
                FmtLog.warn(LOG, "Rename(%s, %s): Patch log of name %s not found", oldName, newName, oldName);
                throw new DeltaException("Can't rename log - PatchLog '"+oldName+"' not registered");
            }
            if ( dataSourceRegistry.containsName(newName) ) {
                // This may happen when operations are retried.
                FmtLog.warn(LOG, "Rename(%s, %s): Patch log of name %s already exists", oldName, newName, newName);
                throw new DeltaException("Can't rename log - PatchLog new name '"+oldName+"' exists");
            }
            PatchLog newPatchLog = renamePatchLog(patchLog, oldName, newName);
            // registry changes done in LocalServer.renameDataSource
            return newPatchLog;
        }
    }

    // The xsd:dateTime regex (from XMLSchema 1.1, colon become any char ".", reformated)
    private static String xsdRegex =
            "-?([1-9][0-9]{3,}|0[0-9]{3})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])"+
            "T(([01][0-9]|2[0-3]).[0-5][0-9].[0-5][0-9](\\.[0-9]+)?|(24.00.00(\\.0+)?))"+
            "(Z|(\\+|-)((0[0-9]|1[0-3]).[0-5][0-9]|14.00))?";
    private static Pattern pattern = Pattern.compile("/[^/]*(_"+xsdRegex+")$");

    /** Basic copy version - cause change of id and URI*/
    protected PatchLog copyPatchLog(PatchLog patchLog, String oldName, String newName) {
        Id dsRef2 = Id.create();
        String uri = patchLog.getDescription().getUri();
        String uriBase = uri;
        Matcher matcher = pattern.matcher(uriBase);
        // Trim a previous timestamp URI
        if ( matcher.find() )
            uriBase = uriBase.substring(0, matcher.start(1));
        String now = DateTimeUtils.nowAsXSDDateTimeString().replace(':','_');
        String uri2 = uriBase+"_"+now;
        DataSourceDescription dsd2 = new DataSourceDescription(dsRef2, newName, uri2);
        PatchLog patchLog2 = newPatchLog(dsd2);
        PatchLogInfo info = patchLog.getInfo();
        patchLog.range(info.getMinVersion(), info.getMaxVersion()).forEach(patchLog2::append);
        return patchLog2;
    }

    protected PatchLog renamePatchLog(PatchLog patchLog, String oldName, String newName) {
        PatchLog patchLog2 = copyPatchLog(patchLog, oldName, newName);
        delete(patchLog);
        return patchLog2;
    }

    /**
     * Create and properly register a new {@link PatchLog}.
     * Call this to add new patch logs including remote changes.
     * This method calls {@link #newPatchLog} provided by the subclass.
     */
    final
    protected PatchLog createPatchLog(DataSourceDescription dsd) {
        Id dsRef = dsd.getId();
        PatchLog patchLog = newPatchLog(dsd);
        logs.put(dsRef, patchLog);
        if ( dataSourceRegistry != null ) {
            DataSource dataSource = new DataSource(dsd, patchLog);
            dataSourceRegistry.add(dataSource);
        }
        return patchLog;
    }

    /**
     * Release ("delete") the {@link PatchLog}.
     * This call removes both the local registration and the persistent state (in the case of a cluster
     * that means this call is used by the initiator of the removal of a patch log).
     * Called from LocalServer/client request.
     * This method calls {@link #delete} provided by the subclass.
     */
    public void release(PatchLog patchLog) {
        Id dsRef = patchLog.getLogId();
        if ( ! dataSourceRegistry.contains(dsRef) ) {
            FmtLog.warn(LOG, "PatchLog not known to PatchStore: dsRef=%s", dsRef);
            return;
        }
        removeLocalRegistration(patchLog);
        delete(patchLog);
    }

    /** Remove the local registration */
    private void removeLocalRegistration(PatchLog  patchLog) {
        Id dsRef = patchLog.getLogId();
        dataSourceRegistry.remove(dsRef);
        logs.remove(dsRef);
    }

    /**
     * Remove locally and properly de-register a {@link PatchLog}.
     * Call this from subclasses notifying the deletion of a patch log elsewhere.
     * This operation does not clear-up external state.
     */
    final
    protected void notifyDeletionPatchLog(Id dsRef) {
        DataSource ds = dataSourceRegistry.get(dsRef);
        if ( ds == null )
            return ;
        removeLocalRegistration(ds.getPatchLog());
    }

    /**
     * Delete a {@link PatchLog} - remove the persistent state
     * (local server registration is handled elsewhere).
     * @param patchLog
     */
    protected abstract void delete(PatchLog patchLog);
}
