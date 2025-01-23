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

package org.seaborne.delta.server.local.patchstores.filestore;

import static org.seaborne.delta.DeltaConst.F_LOG_TYPE;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.atlas.io.IOX;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.server.local.DataSource;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.patchstores.FileNames;
import org.slf4j.Logger;

/** Various configuration utilities; file-based server. */
public class FileArea {

    private static Logger LOG = Delta.DELTA_LOG;
    /**
     * Look for {@link DataSource DataSources} in a disk area given by {@code location}.
     * <p>
     * Scan the given area for directories (must have a config file), check they are enabled,
     * and deal with {@code log_type}.
     */
    public static List<DataSourceDescription> scanForLogs(Path location) {
        // PatchStore's that rely on the scan of local directories
        Pair<List<Path>, List<Path>> pair = scanDirectory(location);
        List<Path> dataSourcePaths = pair.getLeft();
        List<Path> disabledDataSources = pair.getRight();

        //dataSourcePaths.forEach(p->LOG.info("Data source paths: "+p));
        disabledDataSources.forEach(p->LOG.info("  Data source: "+p+" : Disabled"));

        List<DataSourceDescription> descriptions =
            dataSourcePaths.stream()
                .map(p->{
                    // Extract name from disk name.
                    String dsName = p.getFileName().toString();
                    // read config file.
                    JsonObject sourceObj = JSON.read(p.resolve(FileNames.DS_CONFIG).toString());
                    DataSourceDescription dsd = DataSourceDescription.fromJson(sourceObj);
                    if ( ! Objects.equals(dsName, dsd.getName()) )
                        throw new DeltaConfigException("Names do not match: directory="+dsName+", dsd="+dsd);
                    return dsd;
                })
                .filter(Objects::nonNull)
                .toList();
        return descriptions;
    }

    /**
     * Scan a directory for DataSource areas.
     * May not be formatted properly.
     */
    private static Pair<List<Path>/*enabled*/, List<Path>/*disabled*/> scanDirectory(Path directory) {
        try (Stream<Path> paths = Files.list(directory)) {
            List<Path> directoryEntries = paths.filter(p->Files.isDirectory(p)).sorted().toList();
            List<Path> enabled = directoryEntries.stream()
                .filter(path -> isEnabled(path))
                .collect(Collectors.toList());
            List<Path> disabled = directoryEntries.stream()
                .filter(path -> !isEnabled(path))
                .collect(Collectors.toList());
            return Pair.create(enabled, disabled);
        }
        catch (IOException ex) {
            Log.error(FileArea.class, "Exception while reading "+directory);
            throw IOX.exception(ex);
        }
    }

    /** Test whether a path is to a source are which is enabled (has no disabled marker).
     * This does not check the path leads to a valid source area.
     */
    /*package*/ static boolean isEnabled(Path path) {
        Path disabled = path.resolve(FileNames.DISABLED);
        return ! Files.exists(disabled);
    }

    /** Basic tests - not valid DataSource area but the skeleton of one.
     * Checks it is a directory and has a configuration files.
     */
    /*package*/ static boolean isMinimalDataSource(Path path) {
        if ( ! Files.isDirectory(path) )
            return false ;
        Path cfg = path.resolve(FileNames.DS_CONFIG);
        if ( ! Files.exists(cfg) )
            return false ;
        if ( ! Files.isRegularFile(cfg) )
            FmtLog.warn(Delta.DELTA_LOG, "Data source configuration file name exists but is not a file: %s", cfg);
        if ( ! Files.isReadable(cfg) )
            FmtLog.warn(Delta.DELTA_LOG, "Data source configuration file exists but is not readable: %s", cfg);
        return true ;
    }

//    /** Test for a valid data source - does not check "disabled" */
//    private static boolean xisFormattedDataSource(Path path) {
//        if ( ! FileArea.isMinimalDataSource(path) )
//            return false;
//        // Additional requirements
//        Path patchesArea = path.resolve(FileNames.LOG);
//        if ( ! Files.exists(patchesArea) )
//            return false;
//        // If we keep a state file....
////      Path pathVersion = path.resolve(DPConst.STATE_FILE);
////      if ( ! Files.exists(pathVersion) )
////          return false;
//        return true ;
//    }

    /**
     * Set up a disk file area for the data source
     * @param root
     * @param patchStore
     * @param dsd
     */
    /*package*/ public static Path setupDataSourceByFile(Path root, PatchStore patchStore, DataSourceDescription dsd) {
        // Disk file setup

        Path sourcePath = root.resolve(dsd.getName());

        if ( patchStore.logExists(dsd.getId()) )
            throw new DeltaBadRequestException("DataSource area already exists and is active at: "+sourcePath);

        // Checking.
        // The area can exist, but it must not be formatted for a DataSource
        //        if ( sourceArea.exists() )
        //            throw new DeltaException("Area already exists");

        if ( FileArea.isMinimalDataSource(sourcePath) )
            throw new DeltaBadRequestException("DataSource area already exists at: "+sourcePath);
        if ( ! FileArea.isEnabled(sourcePath) )
            throw new DeltaBadRequestException("DataSource area disabled: "+sourcePath);

        try {
            Files.createDirectories(sourcePath);
        }
        catch (IOException e) {
            throw new DeltaBadRequestException("Failed to create DataSource area: "+sourcePath);
        }

        // Create source.cfg.
        JsonObject obj = dsd.asJson();
        obj.put(F_LOG_TYPE, patchStore.getProvider().getShortName());
        try (OutputStream out = Files.newOutputStream(sourcePath.resolve(FileNames.DS_CONFIG))) {
            JSON.write(out, obj);
        } catch (IOException ex)  { throw IOX.exception(ex); }
        return sourcePath;
    }

    private static final String DELETE_MARKER = "-deleted";

    /** Retire an on-disk log file area */
    public static void retire(Path pathLog) {
        IOX.deleteAll(pathLog);
    }

}
