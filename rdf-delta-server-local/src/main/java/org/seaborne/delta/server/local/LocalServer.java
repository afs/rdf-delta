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

import static org.seaborne.delta.DeltaConst.F_LOG_TYPE ;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection ;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors ;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.ListUtils ;
import org.apache.jena.atlas.lib.Pair ;
import org.apache.jena.atlas.lib.SetUtils ;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.lib.JSONX ;
import org.seaborne.delta.lib.LibX;
import org.seaborne.delta.server.local.patchlog.PatchStore ;
import org.seaborne.delta.server.local.patchlog.PatchStoreFile ;
import org.seaborne.delta.server.local.patchlog.PatchStoreMgr;
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
 *
 */
public class LocalServer {
    private static Logger LOG = LoggerFactory.getLogger(LocalServer.class);

    static { 
        DeltaSystem.init();
        initSystem();
    }
    
    /** After Delta has initialized, make sure some sort of PatchStore provision is set. */ 
    static /*package*/ void initSystem() {
        // Ensure the file-based PatchStore provider is available
        if ( ! PatchStoreMgr.isRegistered(DPS.PatchStoreFileProvider) ) {
            FmtLog.warn(LOG, "PatchStoreFile provider not registered");
            PatchStore ps = new PatchStoreFile();
            if ( ! DPS.PatchStoreFileProvider.equals(ps.getProviderName())) {
                FmtLog.error(LOG, "PatchStoreFile provider name is wrong (expected=%s, got=%s)", DPS.PatchStoreFileProvider, ps.getProviderName());
                throw new DeltaConfigException();
            }
            PatchStoreMgr.register(ps);
        }
        
        // Before anything else, set the default the log provider to "file"
        // This gives a default to a delta.cfg setting.
        if ( PatchStoreMgr.getDftPatchStore() == null ) {
            //FmtLog.warn(LOG, "PatchStore default not set.");
            PatchStoreMgr.setDftPatchStoreName(DPS.PatchStoreFileProvider);
        }
    }
    
    /* File system layout:
     *   Server Root
     *      delta.cfg
     *      /NAME ... per DataSource.
     *          /source.cfg
     *          /Log -- patch on disk (optional)
     *          /data -- TDB database (optional)
     *          /disabled -- if this file is present, then the datasource is not accessible.  
     */
    
    private static Map<Location, LocalServer> servers = new ConcurrentHashMap<>();
    
    private final DataRegistry dataRegistry;
    private final Location serverRoot;
    private final LocalServerConfig serverConfig;
    private static AtomicInteger counter = new AtomicInteger(0);
    // Cache of known disabled data sources. (Not set at startup - disabled
    // data sources are skiped completely so no assumption if a valid format
    // area is made and just the "disabled" file is needed).
    private Set<Id> disabledDatasources = new HashSet<>();
    private Object lock = new Object();
    
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
        List<DataSource> dataSourcesExternal = ListUtils.toList
            (PatchStoreMgr.registered().stream().flatMap(ps->{
                if ( ! ps.callInitFromPersistent(config) )
                    return null;
                return ps.initFromPersistent(config).stream();
            }));
        
        // PatchStore's that rely on the scan of local directories and checking the "log_type" field.        
        Pair<List<Path>, List<Path>> pair = Cfg.scanDirectory(config.getLocation());
        List<Path> dataSourcePaths = pair.getLeft();
        List<Path> disabledDataSources = pair.getRight();
        //dataSourcePaths.forEach(p->LOG.info("Data source paths: "+p));
        disabledDataSources.forEach(p->LOG.info("Data source: "+p+" : Disabled"));
        
        List<DataSource> dataSources = ListUtils.toList
            (dataSourcePaths.stream().map(p->{
                // Extract name from disk name. 
                String dsName = p.getFileName().toString();
                // read config file.
                //DataSource ds = Cfg.makeDataSource(p);
                
                JsonObject sourceObj = JSON.read(p.resolve(DeltaConst.DS_CONFIG).toString());
                // Patch Store provider short name.
                
                String logType = JSONX.getStrOrNull(sourceObj, F_LOG_TYPE);
                String providerName = PatchStoreMgr.shortName2LongName(logType);
                
                DataSourceDescription dsd = DataSourceDescription.fromJson(sourceObj);
                if ( ! Objects.equals(dsName, dsd.getName()) )
                    throw new DeltaConfigException("Names do not match: directory="+dsName+", dsd="+dsd);
                
                if ( providerName == null )
                    throw new DeltaConfigException("No provider name and no default provider: "+dsd);
                
                PatchStore ps = PatchStoreMgr.getPatchStoreByProvider(providerName);
                DataSource ds = DataSource.connect(dsd, ps, p);
                //FmtLog.info(LOG, "  Found %s for %s", ds, ps.getProviderName());

                ps.addDataSource(ds, sourceObj, p);
                if ( LOG.isDebugEnabled() ) 
                    FmtLog.debug(LOG, "DataSource: id=%s, source=%s", ds.getId(), p);
                if ( LOG.isDebugEnabled() ) 
                    FmtLog.debug(LOG, "DataSource: %s (%s)", ds, ds.getName());
                return ds;
            }));

        // Check we found any DataSource only via one route or the other.
        {
            Set<Id> set1 = ids(dataSourcesExternal);
            Set<Id> set2 = ids(dataSources);
            Set<Id> setBoth = SetUtils.intersection(set1, set2);
            if ( ! setBoth.isEmpty() ) {
                // Foudn the same DataSource two ways.
                String msg = "Duplicate DataSources: "+setBoth;
                LOG.warn(msg);
                throw new DeltaConfigException(msg);
            }
        }
        dataSourcesExternal.forEach(ds->dataRegistry.put(ds.getId(), ds));
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
        lServer.initThisServer();
        return lServer ;
    }

    private LocalServer(LocalServerConfig config, DataRegistry dataRegistry) {
        this.serverConfig = config;
        this.dataRegistry = dataRegistry;
        this.serverRoot = config.getLocation();
    }

    private void initThisServer() {
        if ( serverConfig.getLogProvider() != null && PatchStoreMgr.getDftPatchStore() == null )
            PatchStoreMgr.setDftPatchStoreName(serverConfig.getLogProvider());  
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
        dataRegistry.forEach((id, ds)-> x.add(ds.getPatchLog().getDescription()));
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
    
    private static Location dataSourceArea(Location serverRoot, String name) {
        return serverRoot.getSubLocation(name);
    }
    
    /**
     * Create a new data source in the default {@link PatchStore}. This can not
     * be one that has been removed (i.e disabled) whose files must be cleaned
     * up manually.
     */
    public Id createDataSource(String name, String baseURI) {
        PatchStore patchStore = PatchStoreMgr.getDftPatchStore();
        return createDataSource(patchStore, name, baseURI);
    }
    
    /**
     * Create a new data source in the specified {@link PatchStore}. This can
     * not be one that has been removed (i.e disabled) whose files must be
     * cleaned up manually.
     */
    public Id createDataSource(PatchStore patchStore, String name, String baseURI/*, details*/) {
        if ( dataRegistry.containsName(name) )
            throw new DeltaBadRequestException("DataSource with name '"+name+"' already exists");
        Id dsRef = Id.create();
        DataSourceDescription dsd = new DataSourceDescription(dsRef, name, baseURI);
        return createDataSource$(patchStore, dsd);
    }
    
    /**
     * Mark as disabled.
     */
    public void removeDataSource(Id dsRef) {
        // Choose PatchStore then dispatch
        // id -> DataSource -> provider 
        //PatchStore patchStore = null;
        //patchStore.remove(dsRef);
        removeDataSource$(dsRef);
    }
    
    private Id createDataSource$(PatchStore patchStore, DataSourceDescription dsd) {
        synchronized(lock) {
            Path sourcePath = null;
            if ( ! patchStore.isEphemeral() ) {
                Location sourceArea = dataSourceArea(serverRoot, dsd.getName());
                sourcePath = IOX.asPath(sourceArea);

                if ( PatchStore.logExists(dsd.getId()) )
                    throw new DeltaBadRequestException("DataSource area already exists and is active at: "+sourceArea);
                
                // Checking.
                // The area can exist, but it must not be formatted for a DataSource 
                //        if ( sourceArea.exists() )
                //            throw new DeltaException("Area already exists");

                if ( Cfg.isMinimalDataSource(LOG, sourcePath) )
                    throw new DeltaBadRequestException("DataSource area already exists at: "+sourceArea);
                if ( ! Cfg.isEnabled(sourcePath) )
                    throw new DeltaBadRequestException("DataSource area disabled: "+sourceArea);

                String dataDirName = sourceArea.getPath(DeltaConst.DATA);
                if ( FileOps.exists(dataDirName) )
                    throw new DeltaBadRequestException("DataSource area has a likely looking database already");

                // Create source.cfg.
                JsonObject obj = dsd.asJson();
                LOG.info(JSON.toStringFlat(obj));
                try (OutputStream out = Files.newOutputStream(sourcePath.resolve(DeltaConst.DS_CONFIG))) {
                    JSON.write(out, obj);
                } catch (IOException ex)  { throw IOX.exception(ex); }
            }
            // XXX Pass in dsd
            DataSource newDataSource = DataSource.create(dsd.getId(), dsd.getUri(), dsd.getName(), sourcePath);
            dataRegistry.put(dsd.getId(), newDataSource);
            return dsd.getId() ;
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
            // Make inaccessible to getDataSource (for create and remove). 
            dataRegistry.remove(dsRef);
            disabledDatasources.add(dsRef);
            datasource.release();
        }
    }
}
