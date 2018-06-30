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

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors ;

import org.apache.jena.atlas.lib.ListUtils ;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.*;
import org.seaborne.delta.lib.LibX;
import org.seaborne.delta.server.system.DeltaSystem ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A local server.
 * <p>
 * This provides for several {@link DataSource} areas (one per managed patch set - i.e.
 * one per dataset). {@code LocalServer} is responsible for server wide configuration and
 * for the {@link DataSource} lifecycle of create and delete.
 * 
 * @see DeltaLinkLocal
 * @see DataSource
 */
public class LocalServer {
    private static Logger LOG = LoggerFactory.getLogger(LocalServer.class);

    static { 
        DeltaSystem.init();
    }
    
    private static Map<Location, LocalServer> servers = new ConcurrentHashMap<>();
    
    private final DataRegistry dataRegistry;
    private final Location serverRoot;
    private final LocalServerConfig serverConfig;
    private static AtomicInteger counter = new AtomicInteger(0);
    // Cache of known disabled data sources. (Not set at startup - disabled
    // data sources are skipped completely so no assumption if a valid format
    // area is made and just the "disabled" file is needed).
    private Set<Id> disabledDatasources = new HashSet<>();
    private Object lock = new Object();

    private final PatchStore patchStore;
    
    /** Attach to the runtime area for the server. Use "delta.cfg" as the configuration file name.  
     * @param serverRoot
     * @return LocalServer
     */
    public static LocalServer attach(Location serverRoot) {
        return create(serverRoot, DeltaConst.SERVER_CONFIG); 
    }
    
    /** Attach to the runtime area for the server. 
     * Use "delta.cfg" as the configuration file name.
     * Use the directory fo the configuration file as the location.
     * @param confFile  Filename
     * @return LocalServer
     */
    public static LocalServer create(String confFile) {
        LocalServerConfig conf = LocalServerConfig.create()
            .parse(confFile)
            .build();
        return create(conf);
    }
    

    /** Attach to the runtime area for the server.
     * @param serverRoot
     * @param confFile  Filename: absolute filename, or relative to the server process.
     * @return LocalServer
     */
    public static LocalServer create(Location serverRoot, String confFile) {
        confFile = LibX.resolve(serverRoot, confFile);
        LocalServerConfig conf = LocalServerConfig.create()
            .setLocation(serverRoot)
            .parse(confFile)
            .build();
        return create(conf);
    }
    
    /** Create a {@code LocalServer} based on a configuration file. */
    public static LocalServer create(LocalServerConfig conf) {
        Objects.requireNonNull(conf, "Null for configuation");
        if ( conf.getLocation() == null )
            throw new DeltaConfigException("No location");
        if ( servers.containsKey(conf.getLocation()) ) {
            LocalServer server = servers.get(conf.getLocation());
            if ( ! conf.equals(server.getConfig()) )
                throw new DeltaConfigException("Attempt to have two servers, with different configurations for the same file area"); 
            return server;
        }
        LOG.info("Base: "+conf.getLocation());
        DataRegistry dataRegistry = new DataRegistry("Server"+counter.incrementAndGet());
        fillDataRegistry(dataRegistry, conf);
        return attachServer(conf, dataRegistry);
    }
    
    /** Fill a {@link DataRegistry} by:
     * <ul>
     * <li>for any PatchStore that provides the function, call {@code initFromPersistent}
     * <li>scan the server area for directories and deal with {@code log_type}.
     * </ul>
     */
    private static void fillDataRegistry(DataRegistry dataRegistry, LocalServerConfig config) {
        Location location = config.getLocation();

        // PatchStore's that store their state themselves somehow.
        List<DataSource> dataSources = ListUtils.toList
            (PatchStoreMgr.registered().stream().flatMap(ps->{
                if ( ! ps.callInitFromPersistent(config) )
                    return null;
                List<DataSource> dSources = ps.initFromPersistent(config);
                FmtLog.info(LOG, "DataSources: %s : %s", ps.getProvider().getProviderName(), dSources);
                return dSources.stream();
            }));
        dataSources.forEach(ds->dataRegistry.put(ds.getId(), ds));
    }

    private static Set<Id> ids(Collection<DataSource> sources) {
        return sources.stream().map(DataSource::getId).collect(Collectors.toSet());
    }
    
    /** Finish using this {@code LocalServer} */
    public static void release(LocalServer localServer) {
        Location key = localServer.serverConfig.getLocation();
        if ( key != null ) {
            servers.remove(key);
            localServer.shutdown$();
        }
        PatchStore.clearLogIdCache();
    }

    /** For testing */
    public static void releaseAll() {
        servers.forEach((location, server)->server.shutdown());
    }
    
    private static LocalServer attachServer(LocalServerConfig config, DataRegistry dataRegistry) {
        Location loc = config.getLocation();
        if ( ! loc.isMemUnique() ) {
            if ( servers.containsKey(loc) ) {
                LocalServer lServer = servers.get(loc);
                LocalServerConfig config2 = lServer.getConfig();
                if ( Objects.equals(config, config2) ) {
                    return lServer; 
                } else {
                    throw new DeltaException("Attempt to attach to existing location with different configuration: "+loc);
                }
            }
        }
        LocalServer lServer = new LocalServer(config, dataRegistry);
        if ( ! loc.isMemUnique() ) 
            servers.put(loc, lServer);
        return lServer ;
    }

    private LocalServer(LocalServerConfig config, DataRegistry dataRegistry) {
        this.serverConfig = config;
        this.dataRegistry = dataRegistry;
        this.serverRoot = config.getLocation();
        this.patchStore = selectPatchStore(config);
    }
    
    private static PatchStore selectPatchStore(LocalServerConfig config) {
        PatchStore ps = PatchStoreMgr.getPatchStoreByProvider(config.getLogProvider());
        if ( ps == null )
            ps = PatchStoreMgr.getDftPatchStore();
        return ps;
    }

    public void shutdown() {
        LocalServer.release(this);
    }

    private void shutdown$() {
        dataRegistry.clear();
    }

    public DataRegistry getDataRegistry() {
        return dataRegistry;
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
            Path sourcePath = null;
            DataSource newDataSource = DataSource.create(dsd, sourcePath, patchStore);
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
