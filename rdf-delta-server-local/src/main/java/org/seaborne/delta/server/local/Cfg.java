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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.lib.ListUtils;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.Id;
import org.seaborne.delta.lib.IOX;
import org.slf4j.Logger;

/** Various configuration utilities */ 
public class Cfg {

    /** 
     * Scan a directory for datasource areas.
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
    public static boolean isMinimalDataSource(Logger log, Path path) {
        if ( ! Files.isDirectory(path) ) 
            return false ;
        Path cfg = path.resolve(DeltaConst.DS_CONFIG);
        if ( ! Files.exists(cfg) )
            return false ;
        if ( ! Files.isRegularFile(cfg) ) 
            log.warn("Data source configuration file name exists but is not a file: "+cfg);
        if ( ! Files.isReadable(cfg) )
            log.warn("Data source configuration file exists but is not readable: "+cfg);
        return true ;
    }
    
    /** Test for a valid data source - does not check "disabled" */
    private static boolean isFormattedDataSource(Logger log, Path path) {
        if ( ! Cfg.isMinimalDataSource(log, path) )
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
    
    /**
     * Create a {@link DataSource} by reading the "source.cfg" file"
     * @param dataSourceArea The directory where the DataSources details are stored.
     * @return DataSource
     */
    public static DataSource makeDataSource(Path dataSourceArea) {
        JsonObject sourceObj = JSON.read(dataSourceArea.resolve(DeltaConst.DS_CONFIG).toString());
        DataSourceDescription dsd = DataSourceDescription.fromJson(sourceObj);
        Id id = dsd.getId();
        String baseStr = dsd.getName();
        String uriStr = dsd.getUri(); 
        DataSource dataSource = DataSource.connect(id, uriStr, baseStr, dataSourceArea);
        return dataSource ;
    }

}
