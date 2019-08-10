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

import static org.seaborne.delta.DeltaOps.verString;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors ;

import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.*;
import org.seaborne.delta.server.Provider;
import org.seaborne.delta.server.system.DeltaSystem ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A local server.
 * <p>
 * This provides for operations on a {@link PatchStore} for several {@link DataSource}
 * areas using a {@link DataSourceRegistry}.
 * {@code LocalServer} is responsible for server-wide configuration and
 * for the {@link DataSource} lifecycle of create and delete.
 *
 * @see LocalServers
 * @see DeltaLinkLocal
 * @see DataSource
 */
public class LocalServer {
    private static Logger LOG = LoggerFactory.getLogger(LocalServer.class);

    private void devlog(Logger log, String fmt, Object...args) {
        if ( ! log.isDebugEnabled() )
            return;
        if ( label != null )
            fmt = String.format("[%s] %s", label, fmt);
        FmtLog.debug(log, fmt, args);
    }

    static { DeltaSystem.init(); }

    private AtomicBoolean active = new AtomicBoolean(false);

    // Track the LocalServers, - mainly so testing can clear them all.
    private static List<LocalServer> servers = new ArrayList<>();

    private final DataSourceRegistry dataRegistry;
    private final LocalServerConfig serverConfig;
    private static AtomicInteger counter = new AtomicInteger(0);
    // Cache of known disabled data sources. (Not set at startup - disabled
    // data sources are skipped completely so no assumption if a valid format
    // area is made and just the "disabled" file is needed).
    private Set<Id> disabledDatasources = new HashSet<>();
    private Object serverLock = new Object();

    private final PatchStore patchStore;

//    /** Create a {@code LocalServer} using the given configuration file. */
//    public static LocalServer create(String confFile) {
//        LocalServerConfig conf = LocalServerConfig.create()
//            .parse(confFile)
//            .build();
//        return create(conf);
//    }

    /** Create a {@code LocalServer} based on a configuration. */
    public static LocalServer create(LocalServerConfig conf) {
        Objects.requireNonNull(conf, "Null for configuation");
        PatchStore ps = createPatchStore(conf);
        return create(ps, conf);
    }

    private static PatchStore createPatchStore(LocalServerConfig config) {
        Provider provider = config.getLogProvider();
        if ( provider == null )
            throw new DeltaConfigException("LocalServer.selectPatchStore: Provider name is null");
        PatchStoreProvider psp = PatchStoreMgr.getPatchStoreProvider(provider);
        if ( psp == null )
            throw new DeltaConfigException("No patch store provider: "+config.getLogProvider());
        PatchStore ps = psp.create(config);
        if ( ps == null )
            throw new DeltaConfigException("Patch store not created and configured: "+config.getLogProvider());
        return ps;
    }

    /** Create a {@code LocalServer} with a specific {@link PatchStore}. */
    public static LocalServer create(PatchStore ps, LocalServerConfig conf) {
        Objects.requireNonNull(ps, "Null for PatchStore");
        return buildLocalServer(ps, conf).start();
    }

    private static LocalServer buildLocalServer(PatchStore ps, LocalServerConfig conf) {
        DataSourceRegistry dataRegistry = new DataSourceRegistry("Server"+counter.incrementAndGet());
        return newLocalServer(conf, ps, dataRegistry);
    }

    /** Convert {@code DataSource} to {@code Id}. */
    private static Set<Id> ids(Collection<DataSource> sources) {
        return sources.stream().map(DataSource::getId).collect(Collectors.toSet());
    }

    /** Finish using this {@code LocalServer} */
    public static void release(LocalServer localServer) {
        localServer.shutdown$();
        servers.remove(localServer);
    }

    /** For testing */
    public static void releaseAll() {
        servers.forEach(server->server.shutdown$());
        servers.clear();
    }

    /** Make a LocalServer; this includes initializing the patch store */
    private static LocalServer newLocalServer(LocalServerConfig config, PatchStore patchStore, DataSourceRegistry dataRegistry) {
        initializePatchStore(patchStore, dataRegistry, config);
        LocalServer lServer = new LocalServer(config, patchStore, dataRegistry);
        servers.add(lServer);
        return lServer ;
    }

    private static void initializePatchStore(PatchStore ps, DataSourceRegistry dataRegistry, LocalServerConfig config) {
        List<DataSourceDescription> descriptions = ps.initialize(dataRegistry, config);
        FmtLog.info(Delta.DELTA_LOG, "Provider: %s", ps.getProvider().getShortName());
        if ( Delta.DELTA_LOG.isDebugEnabled() )
            descriptions.forEach(dsd->FmtLog.debug(Delta.DELTA_LOG, "  %s", dsd));
        FmtLog.debug(Delta.DELTA_LOG, "DataSources: %s : %s", ps.getProvider().getShortName(), descriptions);
    }

    private static AtomicInteger instancecounter = new AtomicInteger(0);
    private final String label;

    private LocalServer(LocalServerConfig config, PatchStore patchStore, DataSourceRegistry dataRegistry) {
        this.serverConfig = config;
        this.dataRegistry = dataRegistry;
        this.patchStore = patchStore;
        // For multiple localservers in one process.
        this.label = "ls-"+instancecounter.incrementAndGet();
    }

    public void logDetails() {
        // Information.
        List<DataSource> sources = listDataSources();

        if ( sources.isEmpty() )
            FmtLog.info(Delta.DELTA_LOG, "  No data sources");
        else {
            sources.sort( (ds1, ds2)-> ds1.getName().compareTo(ds2.getName()) );
            sources.forEach(ds->{
                PatchLogInfo info = ds.getPatchLog().getInfo();
                FmtLog.info(Delta.DELTA_LOG, "  Data source: %s version [%s,%s]", info.getDataSourceDescr(), verString(info.getMinVersion()), verString(info.getMaxVersion()) );
            });
        }
    }

    public LocalServer start() {
        patchStore.startStore();
        active.set(true);
        return this;
    }

    public void shutdown() {
        active.set(false);
        patchStore.closeStore();
        LocalServer.release(this);
    }

    private void checkActive() {
        if ( ! active.get() )
            throw new DeltaException("LocalServer not active");
    }

    private void shutdown$() {
        dataRegistry.clear();
        getPatchStore().shutdown();
    }

    public DataSourceRegistry getDataRegistry() {
        // Not sync'ed.
        return dataRegistry;
    }

    private DataSourceRegistry syncedDataRegistry() {
        syncPatchStore();
        return dataRegistry;
    }

    public static boolean alwaysSyncPatchStore = false;
    /** Try an action; if it returns null, sync the patch store and try again */
    private <T> T actionSyncPatchStore(Supplier<T> action) {
        if ( alwaysSyncPatchStore )
            syncPatchStore();
        T x = action.get();
        if ( x == null ) {
            //FmtLog.debug(LOG, "[%s] actionSyncPatchStore : sync", label);
            syncPatchStore();
            x = action.get();
            //FmtLog.debug(LOG, "[%s] actionSyncPatchStore : sync -> %s", label, x);
        } else {
            //FmtLog.info(LOG, "[%s] actionSyncPatchStore : cache : %s", label, x);
        }
        return x;
    }

    private void syncPatchStore() {
        patchStore.sync();
    }

    public PatchStore getPatchStore() {
        return patchStore;
    }

    public DataSource getDataSource(Id dsRef) {
        // Called to poll for a logs version changes.
        devlog(LOG, "getDataSource(%s)", dsRef);
        checkActive();
        DataSource ds = actionSyncPatchStore(()->dataRegistry.get(dsRef));
        return dataSource(ds);
    }

    public DataSource getDataSourceByName(String name) {
        devlog(LOG, "getDataSourceByName(%s)", name);
        checkActive();
        DataSource ds = actionSyncPatchStore(()->dataRegistry.getByName(name));
        return dataSource(ds);
    }

    public DataSource getDataSourceByURI(String uri) {
        devlog(LOG, "getDataSourceByURI(%s)", uri);
        checkActive();
        DataSource ds = actionSyncPatchStore(()->dataRegistry.getByURI(uri));
        return dataSource(ds);
    }

    private DataSource dataSource(DataSource ds) {
        if ( ds == null )
            return null;
        if ( disabledDatasources.contains(ds.getId()) )
            return null;
        return ds;
    }

    public DataSourceDescription getDescriptor(Id dsRef) {
        checkActive();
        DataSource dataSource = actionSyncPatchStore(()->dataRegistry.get(dsRef));
        return descriptor(dataSource);
    }

    private DataSourceDescription descriptor(DataSource dataSource) {
        DataSourceDescription descr = new DataSourceDescription
            (dataSource.getId(),
             dataSource.getURI(),
             dataSource.getName());
        return descr;
    }

    /** Get the LocalServerConfig use for this server */
    public LocalServerConfig getConfig() {
        return serverConfig;
    }

    public List<Id> listDataSourcesIds() {
        checkActive();
        return new ArrayList<>(syncedDataRegistry().keys());
    }

    public List<DataSource> listDataSources() {
        checkActive();
        List<DataSource> x = new ArrayList<>();
        syncedDataRegistry().forEach((id, ds)-> x.add(ds));
        return x;
    }

    public List<PatchLogInfo> listPatchLogInfo() {
        // Called to poll for patch log create/delete
        checkActive();
        List<PatchLogInfo> x = new ArrayList<>();
        syncedDataRegistry().forEach((id, ds)-> x.add(ds.getPatchLog().getInfo()));
        return x;
      }

    /**
     * Create a new data source in the default {@link PatchStore}. This can not
     * be one that has been removed (i.e disabled) whose files must be cleaned
     * up manually.
     */
    public Id createDataSource(String name, String baseURI) {
        return createDataSource(patchStore, name, baseURI);
    }

    private static AtomicInteger createCounter = new AtomicInteger(0);
    /**
     * Create a new data source in the specified {@link PatchStore}. This can
     * not be one that has been removed (i.e. disabled) whose files must be
     * cleaned up manually.
     */
    public Id createDataSource(PatchStore patchStore, String name, String baseURI/*, details*/) {
        checkActive();
        int C = createCounter.incrementAndGet();
        if ( syncedDataRegistry().containsName(name) )
            throw new DeltaBadRequestException("DataSource with name '"+name+"' already exists");
        // Proposed id, only becomes permanent if the create actually does a create and
        // does not find someone else has (with a different id);
        // XXX Delay id allocation until create-for-real. Create does not take a DSD.
        Id dsRef = Id.create();
        devlog(LOG, "(%d) createDataSource/start: %s", C, dsRef);
        DataSourceDescription dsd = new DataSourceDescription(dsRef, name, baseURI);
        DataSource dataSource = createDataSource$(C, patchStore, dsd);
        devlog(LOG, "(%d) createDataSource/finish: %s %s", C, dsRef, dataSource.getDescription());
        // dsRef is not invalid if it was not used.
        dsRef = null;
        return dataSource.getId();
    }

    private DataSource createDataSource$(int C, PatchStore patchStore, DataSourceDescription dsd) {
        synchronized(serverLock) {
            // Server lock, not cluster lock.

            DataSourceRegistry reg = syncedDataRegistry();
            if ( reg.containsName(dsd.getName()) ) {
                FmtLog.info(LOG, "(%d) Existing: %s", C, dsd);
                DataSource ds = reg.getByName(dsd.getName());
                return ds;
            }

            PatchLog patchLog = patchStore.createLog(dsd);
            // -- DEV
            // May not be id in DSD - the prev test was not in a cluster lock but patchStore.createLog is atomic ("createOrGet")
            if ( ! patchLog.getLogId().equals(dsd.getId()) ) {
                if ( reg.containsName(dsd.getName()) ) {
                    devlog(LOG, "(%d) Existing1: %s -> %s", C, dsd, patchLog.getDescription());
                    DataSource ds = reg.getByName(dsd.getName());
                    devlog(LOG, "(%d) Existing2: %s -> %s", C, dsd, ds.getDescription());
                    return ds;
                }
                LOG.error("Existing but not found again: "+dsd+" : patch ="+patchLog.getDescription());
            }
            // -- DEV
            devlog(LOG, "(%d) New: %s", C, dsd);
            DataSource newDataSource = new DataSource(dsd, patchLog);
            // XXX Isn't this done in PatchStore.createPatchLog as well?
            reg.put(dsd.getId(), newDataSource);
            return newDataSource;
        }
    }

   /** Remove from active use.*/
    public void removeDataSource(Id dsRef) {
        checkActive();
        removeDataSource$(dsRef);
    }

private void removeDataSource$(Id dsRef) {
        DataSource datasource1 = getDataSource(dsRef);
        if ( datasource1 == null )
            return;

        // Lock with create.
        synchronized(serverLock) {
            DataSource datasource = getDataSource(dsRef);
            if ( datasource == null )
                return;
            // Done in PatchStrore.release -- dataRegistry.remove
            PatchStore patchStore = datasource.getPatchStore();
            // This does the dataRegsitry remove.
            patchStore.release(datasource.getPatchLog());
            disabledDatasources.add(dsRef);
        }
    }
}
