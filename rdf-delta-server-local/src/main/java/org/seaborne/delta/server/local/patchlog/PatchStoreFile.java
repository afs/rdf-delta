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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.atlas.json.JsonObject ;
import org.apache.jena.atlas.lib.NotImplemented ;
import org.apache.jena.tdb.base.file.Location ;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaConst ;
import org.seaborne.delta.lib.IOX ;
import org.seaborne.delta.server.local.DPS ;
import org.seaborne.delta.server.local.DataSource;
import org.seaborne.delta.server.local.LocalServerConfig;

/** A {@code PatchStore} storing patches in a {@link FileStore} */  
public class PatchStoreFile extends PatchStore {

    // We manage ...
    private Set<DataSourceDescription> sources = ConcurrentHashMap.newKeySet(); 
    
    public static void registerPatchStoreFile() {
        PatchStore ps = new PatchStoreFile();
        PatchStoreMgr.registerShortName(DeltaConst.LOG_FILE, ps.getProviderName());
        PatchStoreMgr.register(ps);
    }
    
    public PatchStoreFile() {
        super(DPS.PatchStoreFileProvider) ;
    }

    @Override
    protected PatchLogFile create(DataSourceDescription dsd, Path dsPath) {
        Path logPath = dsPath.resolve(DeltaConst.LOG);
        IOX.ensureDirectory(logPath);
        Location loc = Location.create(logPath.toString());
        PatchLogFile patchLog = PatchLogFile.attach(dsd, loc);
        sources.add(dsd); 
        return patchLog ;
    }

    @Override
    public List<DataSourceDescription> listDataSources() {
        return new ArrayList<>(sources);
    }

    @Override
    public void addDataSource(DataSource ds, JsonObject sourceObj, Path dataSourceArea) {
        sources.add(ds.getDescription());
    }

    @Override
    public List<DataSource> initFromPersistent(LocalServerConfig config) {
        boolean deftToHere = DPS.PatchStoreFileProvider.equals(config.getLogProvider());
        throw new NotImplemented();
    }
    
    @Override
    public boolean callInitFromPersistent(LocalServerConfig config) {
        // Rely on LocalServer scan for retained, but state loosing, patchstores.  
        return false ;
    }
    
//    @Override
//    public List<DataSource> initFromPersistent(LocalServerConfig config) {
//        config.getLogProvider();
//        Pair<List<Path>, List<Path>> pair = Cfg.scanDirectory(config.getLocation());
//        List<Path> dataSourcePaths = pair.getLeft();
//        List<Path> disabledDataSources = pair.getRight();
//
//        //dataSourcesPaths.forEach(p->LOG.info("Data source: "+p));
//        disabledDataSources.forEach(p->LOG.info("Data source: "+p+" : Disabled"));
//        
//        List<DataSource> dataSources = ListUtils.toList
//          (dataSourcePaths.stream().map(p->{
//              // Extract name from disk name. 
//              String dsName = p.getFileName().toString();
//                             
//              // Config file.
//              // Check provider.
//              // Make DataSource
//              
//              // Break up. DRY with LocalServer.
//              DataSource ds = null ; //Cfg.makeDataSource(p);
//              
//              if ( LOG.isDebugEnabled() ) 
//                  FmtLog.debug(LOG, "DataSource: id=%s, source=%s", ds.getId(), p);
//              if ( LOG.isDebugEnabled() ) 
//                  FmtLog.debug(LOG, "DataSource: %s (%s)", ds, ds.getName());
//              
//              sources.add(ds.getDescription());
//              return ds;
//          })); 
//      return dataSources;
//    }
    
    

    
    // Add checking for log_type.
//    @Override
//    public List<DataSource> initFromPersistent(LocalServerConfig config) {
//        Pair<List<Path>, List<Path>> pair = Cfg.scanDirectory(config.getLocation());
//        List<Path> dataSourcePaths = pair.getLeft();
//        List<Path> disabledDataSources = pair.getRight();
//
//        //dataSourcesPaths.forEach(p->LOG.info("Data source: "+p));
//        disabledDataSources.forEach(p->LOG.info("Data source: "+p+" : Disabled"));
//
//        List<DataSource> dataSources = ListUtils.toList
//            (dataSourcePaths.stream().map(p->{
//                // Extract name from disk name. 
//                String dsName = p.getFileName().toString();
//                DataSource ds = Cfg.makeDataSource(p);
//                
//                if ( LOG.isDebugEnabled() ) 
//                    FmtLog.debug(LOG, "DataSource: id=%s, source=%s", ds.getId(), p);
//                if ( LOG.isDebugEnabled() ) 
//                    FmtLog.debug(LOG, "DataSource: %s (%s)", ds, ds.getName());
//                
//                sources.add(ds.getDescription());
//                return ds;
//            })); 
//        return dataSources;
//    }
}
