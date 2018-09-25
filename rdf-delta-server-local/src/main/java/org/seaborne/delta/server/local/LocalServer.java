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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors ;

import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.server.system.DeltaSystem ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A local server.
 * <p>
 * This provides for operations on a {@link PatchStore} for several {@link DataSource}
 * areas using a {@link DataRegistry}.
 * {@code LocalServer} is responsible for server-wide configuration and
 * for the {@link DataSource} lifecycle of create and delete.
 *
 * @see LocalServers
 * @see DeltaLinkLocal
 * @see DataSource
 */
public class LocalServer {
    private static Logger LOG = LoggerFactory.getLogger(LocalServer.class);

    static { DeltaSystem.init(); }

    // Track the LocalServers, - mainly so testing can clear them all.
    private static List<LocalServer> servers = new ArrayList<>();

    private final DataRegistry dataRegistry;
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
        String providerName = config.getLogProvider();
        if ( providerName == null )
            throw new DeltaConfigException("LocalServer.selectPatchStore: Provider name is null");
        PatchStoreProvider psp = PatchStoreMgr.getPatchStoreProvider(providerName);
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
        DataRegistry dataRegistry = new DataRegistry("Server"+counter.incrementAndGet());
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
    private static LocalServer newLocalServer(LocalServerConfig config, PatchStore patchStore, DataRegistry dataRegistry) {
        initializePatchStore(patchStore, dataRegistry, config);
        LocalServer lServer = new LocalServer(config, patchStore, dataRegistry);
        servers.add(lServer);
        return lServer ;
    }

    private static void initializePatchStore(PatchStore ps, DataRegistry dataRegistry, LocalServerConfig config) {
        List<DataSourceDescription> descriptions = ps.initialize(dataRegistry, config);
        FmtLog.info(LOG, "DataSources: %s : %s", ps.getProvider().getShortName(), descriptions);
    }

    private LocalServer(LocalServerConfig config, PatchStore patchStore, DataRegistry dataRegistry) {
        this.serverConfig = config;
        this.dataRegistry = dataRegistry;
        this.patchStore = patchStore;

    }

    public void shutdown() {
        LocalServer.release(this);
    }

    private void shutdown$() {
        dataRegistry.clear();
        getPatchStore().shutdown();
    }

    public DataRegistry getDataRegistry() {
        // Not sync'ed.
        return dataRegistry;
    }

    private DataRegistry syncedDataRegistry() {
        syncPatchStore();
        return dataRegistry;
    }

    /** Try an action; if it returns null, sync the patch store and try again */
    private <T> T actionSyncPatchStore(Supplier<T> action) {
        T x = action.get();
        if ( x == null ) {
            syncPatchStore();
            x = action.get();
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
        DataSource ds = actionSyncPatchStore(()->dataRegistry.get(dsRef));
        return dataSource(ds);
    }

    public DataSource getDataSourceByName(String name) {
        DataSource ds = actionSyncPatchStore(()->dataRegistry.getByName(name));
        return dataSource(ds);
    }

    public DataSource getDataSourceByURI(String uri) {
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
        return new ArrayList<>(syncedDataRegistry().keys());
    }

    public List<DataSource> listDataSources() {
        List<DataSource> x = new ArrayList<>();
        syncedDataRegistry().forEach((id, ds)-> x.add(ds));
        return x;
    }

    public List<PatchLogInfo> listPatchLogInfo() {
        // Important enough to have it's own cut-through method.
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

    /**
     * Create a new data source in the specified {@link PatchStore}. This can
     * not be one that has been removed (i.e. disabled) whose files must be
     * cleaned up manually.
     */
    public Id createDataSource(PatchStore patchStore, String name, String baseURI/*, details*/) {
        if ( syncedDataRegistry().containsName(name) )
            throw new DeltaBadRequestException("DataSource with name '"+name+"' already exists");
        Id dsRef = Id.create();
        DataSourceDescription dsd = new DataSourceDescription(dsRef, name, baseURI);
        DataSource dataSource = createDataSource$(patchStore, dsd);
        return dataSource.getId();
    }

    /** Remove from active use.*/
    public void removeDataSource(Id dsRef) {
        removeDataSource$(dsRef);
    }

    private DataSource createDataSource$(PatchStore patchStore, DataSourceDescription dsd) {
        synchronized(serverLock) {
            DataRegistry reg = syncedDataRegistry();
            PatchLog patchLog = patchStore.createLog(dsd);
            DataSource newDataSource = new DataSource(dsd, patchLog);
            // XXX Isn't this done in PatchStore.createPatchLog as well?
            reg.put(dsd.getId(), newDataSource);
            return newDataSource;
        }
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
