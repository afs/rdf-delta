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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors ;

import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.*;
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

    static {
        DeltaSystem.init();
    }
    
    // Track the LocalServers, - mainly so testing are clear them all. 
    private static List<LocalServer> servers = new ArrayList<>();
    
    private final DataRegistry dataRegistry;
    private final LocalServerConfig serverConfig;
    private static AtomicInteger counter = new AtomicInteger(0);
    // Cache of known disabled data sources. (Not set at startup - disabled
    // data sources are skipped completely so no assumption if a valid format
    // area is made and just the "disabled" file is needed).
    private Set<Id> disabledDatasources = new HashSet<>();
    private Object lock = new Object();

    private final PatchStore patchStore;
    
    /** Create a {@code LocalServer} with default setup. */ 
    public static LocalServer create() {
        LocalServerConfig conf = LocalServerConfig.create().build();
        return create(conf);
    }
    
    /** Create a {@code LocalServer} using the given configuration file. */
    public static LocalServer create(String confFile) {
        LocalServerConfig conf = LocalServerConfig.create()
            .parse(confFile)
            .build();
        return create(conf);
    }

    /** Create a {@code LocalServer} based on a configuration. */
    public static LocalServer create(LocalServerConfig conf) {
        Objects.requireNonNull(conf, "Null for configuation");
        PatchStore ps = selectPatchStore(conf);
        return create(ps, conf);
    }
    
    private static PatchStore selectPatchStore(LocalServerConfig config) {
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
        fillDataRegistry(ps, dataRegistry, conf);
        return localServer(conf, ps, dataRegistry);
    }

    /**
     * Fill a {@link DataRegistry} by initializing the {@link PatchStore PatchStores}
     * that provides the function, call {@code initFromPersistent}.
     * @param ps 
     */
    private static void fillDataRegistry(PatchStore ps, DataRegistry dataRegistry, LocalServerConfig config) {
        if ( ! ps.callInitFromPersistent(config) )
            return;
        List<DataSource> dataSources = ps.initFromPersistent(config);
        FmtLog.info(LOG, "DataSources: %s : %s", ps.getProvider().getProviderName(), dataSources);
        dataSources.forEach(ds->dataRegistry.put(ds.getId(), ds));
    }

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
    
    private static LocalServer localServer(LocalServerConfig config, PatchStore patchStore, DataRegistry dataRegistry) {
        LocalServer lServer = new LocalServer(config, patchStore, dataRegistry);
        servers.add(lServer);
        return lServer ;
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
        // Implicitly, one LocalServer per JVM.
        dataRegistry.clear();
    }

    public DataRegistry getDataRegistry() {
        return dataRegistry;
    }
    
    public PatchStore getPatchStore() {
        return patchStore;
    }
    
    public DataSource getDataSource(Id dsRef) {
        DataSource ds = dataRegistry.get(dsRef);
        return dataSource(ds);
    }

    public DataSource getDataSourceByName(String name) {
        DataSource ds = dataRegistry.getByName(name);
        return dataSource(ds);
    }
    
    public DataSource getDataSourceByURI(String uri) {
        DataSource ds = dataRegistry.getByURI(uri);
        return dataSource(ds);
    }

    private DataSource dataSource(DataSource ds) {
        if ( ds == null )
            return null;
        if ( disabledDatasources.contains(ds.getId()) )
            return null;
        return ds;
    }
    
    /** Get the LocalServerConfig use for this server */
    public LocalServerConfig getConfig() {
        return serverConfig;
    }

    public List<Id> listDataSourcesIds() {
        return new ArrayList<>(dataRegistry.keys());
    }
    
    public List<DataSource> listDataSources() {
      List<DataSource> x = new ArrayList<>();
      dataRegistry.forEach((id, ds)-> x.add(ds));
      return x;
    }

    public List<PatchLogInfo> listPatchLogInfo() {
        // Important enough to have it's own cut-through method. 
        List<PatchLogInfo> x = new ArrayList<>();
        dataRegistry.forEach((id, ds)-> x.add(ds.getPatchLog().getInfo()));
        return x;
      }

    public DataSourceDescription getDescriptor(Id dsRef) {
        DataSource dataSource = dataRegistry.get(dsRef);
        return descriptor(dataSource);
    }
    
    private DataSourceDescription descriptor(DataSource dataSource) {
        DataSourceDescription descr = new DataSourceDescription
            (dataSource.getId(),
             dataSource.getURI(),
             dataSource.getName());
        return descr;
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
        if ( dataRegistry.containsName(name) )
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
    
    // Depends on the on-disk stub for a PatchLog. 
    private DataSource createDataSource$(PatchStore patchStore, DataSourceDescription dsd) {
        synchronized(lock) {
            DataSource newDataSource = DataSource.create(dsd, patchStore);
            dataRegistry.put(dsd.getId(), newDataSource);
            return newDataSource;
        }
    }
    
   private void removeDataSource$(Id dsRef) {
        DataSource datasource1 = getDataSource(dsRef);
        if ( datasource1 == null )
            return;
        // Lock with create.
        synchronized(lock) {
            DataSource datasource = getDataSource(dsRef);
            if ( datasource == null )
                return;
            PatchStore patchStore = datasource.getPatchStore();
            dataRegistry.remove(dsRef);
            disabledDatasources.add(dsRef);
            datasource.release();
        }
    }
}
