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

import static org.seaborne.delta.DeltaConst.F_LOG_TYPE;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.ListUtils;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.*;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.lib.JSONX;
import org.slf4j.Logger;

/** Various configuration utilities; file-based server. */ 
public class Cfg {

    /** 
     * Look for {@link DataSource DataSources} in a disk area given by {@code location}.
     * <p>   
     * Scan the given area for directories (must have a config file),
     * check they are enabled,
     * 
     * 
     *  and deal with {@code log_type}.
     * <p>
     * Use with {@link LocalServer} when backed by disk.
     */
    public static List<DataSource> scanForDataSources(Location location, Logger LOG) {
        // PatchStore's that rely on the scan of local directories and checking the "log_type" field.        
        Pair<List<Path>, List<Path>> pair = scanDirectory(location);
        List<Path> dataSourcePaths = pair.getLeft();
        List<Path> disabledDataSources = pair.getRight();
        
        //dataSourcePaths.forEach(p->LOG.info("Data source paths: "+p));
        disabledDataSources.forEach(p->LOG.info("Data source: "+p+" : Disabled"));
        
        List<DataSource> dataSources = ListUtils.toList
            (dataSourcePaths.stream().map(p->{
                // Extract name from disk name. 
                String dsName = p.getFileName().toString();
                // read config file.
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
                if ( LOG.isDebugEnabled() ) 
                    FmtLog.debug(LOG, "DataSource: id=%s, source=%s", ds.getId(), p);
                if ( LOG.isDebugEnabled() ) 
                    FmtLog.debug(LOG, "DataSource: %s (%s)", ds, ds.getName());
                return ds;
            }));
        return dataSources;
    }

    /** 
     * Scan a directory for DataSource areas.
     * These must have a file called source.cfg.
     */
    public static Pair<List<Path>/*enabled*/, List<Path>/*disabled*/> scanDirectory(Location serverRoot) {
        Path dir = IOX.asPath(serverRoot);

        try {
            List<Path> directory = ListUtils.toList( Files.list(dir).filter(p->Files.isDirectory(p)).sorted() );
//            directory.stream()
//                .filter(LocalServer::isFormattedDataSource)
//                .collect(Collectors.toList());
            List<Path> enabled = directory.stream()
                .filter(path -> isEnabled(path))
                .collect(Collectors.toList());
            List<Path> disabled = directory.stream()
                .filter(path -> !isEnabled(path))
                .collect(Collectors.toList());
            return Pair.create(enabled, disabled);
        }
        catch (IOException ex) {
            Log.error(Cfg.class, "Exception while reading "+dir);
            throw IOX.exception(ex);
        }
    }

    /** Test whether a path is to a source are which is enabled (has no disabled marker).
     * This does not check the path leads to a valid source area.
     */
    public static boolean isEnabled(Path path) {
        Path disabled = path.resolve(DeltaConst.DISABLED);
        return ! Files.exists(disabled);
    }

    /** Basic tests - not valid DataSource area but the skeleton of one.
     * Checks it is a directory and has a configuration files.
     */
    public static boolean isMinimalDataSource(Path path) {
        if ( ! Files.isDirectory(path) ) 
            return false ;
        Path cfg = path.resolve(DeltaConst.DS_CONFIG);
        if ( ! Files.exists(cfg) )
            return false ;
        if ( ! Files.isRegularFile(cfg) ) 
            FmtLog.warn(Delta.DELTA_LOG, "Data source configuration file name exists but is not a file: %s", cfg);
        if ( ! Files.isReadable(cfg) )
            FmtLog.warn(Delta.DELTA_LOG, "Data source configuration file exists but is not readable: %s", cfg);
        return true ;
    }
    
    /** Test for a valid data source - does not check "disabled" */
    private static boolean isFormattedDataSource(Path path) {
        if ( ! Cfg.isMinimalDataSource(path) )
            return false;
        // Additional requirements
        Path patchesArea = path.resolve(DeltaConst.LOG);
        if ( ! Files.exists(patchesArea) )
            return false;
        // If we keep a state file....
//      Path pathVersion = path.resolve(DPConst.STATE_FILE);
//      if ( ! Files.exists(pathVersion) )
//          return false;
        return true ;
    }
    
    
    
    private static Logger LOG = Delta.DELTA_LOG;
    /** Set up a disk file area for the data source 
     * @param patchStore */
    /*package*/ static Path setupDataSourceByFile(Location root, PatchStore patchStore, DataSourceDescription dsd) {
        // Disk file setup.
        // Eventually this fixed code needs to move to PatchStoreFile or a library and be invoked from PatchStoreFile. 
        
        Location sourceArea = root.getSubLocation(dsd.getName());
        Path sourcePath = IOX.asPath(sourceArea);

        if ( PatchStore.logExists(dsd.getId()) )
            throw new DeltaBadRequestException("DataSource area already exists and is active at: "+sourceArea);
        
        // Checking.
        // The area can exist, but it must not be formatted for a DataSource 
        //        if ( sourceArea.exists() )
        //            throw new DeltaException("Area already exists");

        if ( Cfg.isMinimalDataSource(sourcePath) )
            throw new DeltaBadRequestException("DataSource area already exists at: "+sourceArea);
        if ( ! Cfg.isEnabled(sourcePath) )
            throw new DeltaBadRequestException("DataSource area disabled: "+sourceArea);

        // Create source.cfg.
        JsonObject obj = dsd.asJson();
        obj.put(F_LOG_TYPE, patchStore.getProvider().getShortName());
        LOG.info(JSON.toStringFlat(obj));
        try (OutputStream out = Files.newOutputStream(sourcePath.resolve(DeltaConst.DS_CONFIG))) {
            JSON.write(out, obj);
        } catch (IOException ex)  { throw IOX.exception(ex); }
        return sourcePath;
    }
    
    private static final String DELETE_MARKER = "-deleted";

    /** Retire an on-disk log file area */ 
    /*package*/ static void retire(Path pathLog) {
        if ( true ) {
            // Mark unavailable.
            Path disabled = pathLog.resolve(DeltaConst.DISABLED);
            try { Files.createFile(disabled); } 
            catch (IOException ex) { throw IOX.exception(ex); }
        }
        if ( true ) {
            // Move to "NAME-deleted-N"
            Path dest = IOX.uniqueDerivedPath(pathLog, (x)->x+DELETE_MARKER);
            try { Files.move(pathLog, dest, StandardCopyOption.ATOMIC_MOVE); }
            catch (IOException e) { throw IOX.exception(e); }
        }
        if ( false ) {
            // Destroy.
            try {
                Files.walk(pathLog)
                    // So a directory path itself is after its entries. 
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            }
            catch (IOException e) { throw IOX.exception(e); }
            File d = pathLog.toFile();
            FileOps.clearAll(d);
            d.delete();
        }

    }
 
}
