package org.seaborne.delta.client;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption ;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern ;
import java.util.stream.Collectors;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.NotImplemented ;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.DatasetGraphFactory ;
import org.apache.jena.tdb.TDBFactory ;
import org.apache.jena.tdb.base.file.Location;
import org.apache.jena.tdb2.DatabaseMgr;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.Id;
import org.seaborne.delta.PersistentState;
import org.seaborne.delta.Version;
import org.seaborne.delta.lib.IOX;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A "Zone" is a collection of named data sources. It tracks their state {@link DataState}
 * across JVM restarts. It does not manage the RDF contents.
 */
public class Zone {
    private static final Logger LOG = LoggerFactory.getLogger(Zone.class);
    private static final String DELETE_MARKER = "-deleted";

    // Zone state
    private volatile boolean           INITIALIZED   = false ;
    private Map<Id, DataState>         states        = new ConcurrentHashMap<>();
    // For datasets create within the zone.
    private Map<Id, DatasetGraph>      datasets      = new ConcurrentHashMap<>();
    // For external datasets
    private Map<Id, DatasetGraph>      external      = new ConcurrentHashMap<>();
    private Map<String, Id>            names         = new ConcurrentHashMap<>();
    private Path                       stateArea     = null;
    private final Location             stateLocation;
    private Object                     zoneLock      = new Object();

    private static Map<Location, Zone> zones         = new ConcurrentHashMap<>();

    /** Create a zone; connect to an existing one if it exists in the JVM or on-disk */
    // XXX Rename
    public static Zone connect(String area) {
        Location location = (area == null) ? Location.mem() : Location.create(area);
        return connect(location);
    }

    /** Create a zone; connect to an existing one if it exists in the JVM or on-disk */
    // XXX Rename
    public static Zone connect(Location area) {
        synchronized(zones) {
            if ( zones.containsKey(area) )
                return zones.get(area);
            Zone zone = new Zone(area);
            zones.put(area, zone);
            return zone;
        }
    }

    private Zone(Location area) {
        this.stateLocation = area;
        init();
    }

    /** Return the zone for this area if it exists in the JVM. */
    public static Zone get(String area) {
        return get(Location.create(area));
    }

    /** Return the locations of all zones. */
    public static Collection<String> zones() {
        return zones.keySet().stream().map(Location::getDirectoryPath).collect(Collectors.toSet());
    }


    /** Return the zone for this area if it exists in the JVM. */
    public static Zone get(Location area) {
        return zones.getOrDefault(area, null);
    }

    /** Clear the cache - for testing */
    public static void clearZoneCache() {
        zones.clear();
    }

    private void reset() {
        states.clear();
        datasets.clear();
        external.clear();
        names.clear();
        // Rescan?
    }

    /**
     * Reset to the uninitialized state.
     * Should not be needed in normal operation; mainly for testing.
     */
    public void shutdown() {
        synchronized(zoneLock) {
            reset();
            stateArea = null;
            INITIALIZED = false;
            zones.remove(stateLocation);
        }
    }

    /** Ids of connections active in this zone */
    public List<Id> localConnections() {
        return new ArrayList<>(states.keySet());
    }

    private void init() {
        if ( INITIALIZED ) {
            checkInit(stateLocation);
            return;
        }
        synchronized(zoneLock) {
            if ( INITIALIZED ) {
                checkInit(stateLocation);
                return;
            }
            INITIALIZED = true;
            if ( stateLocation == null || stateLocation.isMem() ) {
                // In-memory only.
                stateArea = null;
                return ;
            }
            stateArea = IOX.asPath(stateLocation);
            List<Path> x = scanForDataState(stateLocation);
            x.forEach(p->LOG.info("Connection : "+p));
            x.forEach(p->fromOnDiskState(p));
        }
    }

    /** Read from disk and register - adjust version for ephemeral */
    private void fromOnDiskState(Path p) {
        DataState dataState = readDataState(p);
        if ( dataState.getStorageType().isEphemeral() )
            // If ephemeral, force version to 0.
            dataState.updateState(Version.INIT, null);
        try {
            register(dataState);
        } catch (Exception ex) {
            LOG.error("Problem registering and restoring from path "+p, ex);
            // And (try to) continue.
        }
    }

    private void register(DataState dataState) {
        Id dsRef = dataState.getDataSourceId();
        DatasetGraph dsg = localStorage(dataState.getStorageType(), dataPath(dataState));
        if ( dsg != null )
            datasets.put(dsRef, dsg);
        states.put(dsRef, dataState);
        names.put(dataState.getDatasourceName(), dsRef);
    }

    private boolean isInitialized() {
        return INITIALIZED;
    }

    private void checkInit(Location area) {
        if ( ! Objects.equals(stateLocation, area) )
            throw new DeltaException("Attempt to reinitialize the Zone: "+stateArea+" => "+area);
    }

    /** Is there an area already? */
    public boolean exists(Id dsRef) {
        // Rename as registered?
        return states.containsKey(dsRef);
    }

    public DataState getExisting(String name) {
        Id id = getIdForName(name);

        if ( id == null ) {
            // look on disk.
            Path dsState = stateArea.resolve(name);
            if ( Files.exists(dsState) ) {
                DataState state = readDataState(dsState);
                return state;
                // see scanForDataState
            }
            return null;
        } else {
            return states.get(id);
        }
    }

    /** Initialize a new area. */
    public DataState create(Id dsRef, String name, String uri, LocalStorageType storage) {
        Objects.requireNonNull(dsRef,   "Data source reference");
        Objects.requireNonNull(name,    "Data source name");
        Objects.requireNonNull(storage, "Storage type");

        Path statePath = null;
        Path dataPath = null;
        if ( stateArea != null ) {
            // Per data source area.
            Path conn = stateArea.resolve(name);
            FileOps.ensureDir(conn.toString());

            // {zone}/{name}/state
            // Always write the datastate even if ephemeral.
            statePath = conn.resolve(FN.STATE);
            // {zone}/{name}/data
            dataPath = storage.isEphemeral() ? null : conn.resolve(FN.DATA);
        }

        synchronized (zoneLock) {
            if ( ! INITIALIZED )
                throw new DeltaException("Not initialized");
            if ( states.containsKey(dsRef) )
                throw new DeltaException("Already exists: data state for " + dsRef + " : name=" + name);
            if ( dataPath != null )
                FileOps.ensureDir(dataPath.toString());
            // statePath is null for ephemeral

            DataState dataState = new DataState(this, statePath, storage, dsRef, name, uri, Version.INIT, null);
            register(dataState);
            return dataState;
        }
    }

    private Path stateArea(DataState dataState) {
        return stateArea.resolve(dataState.getDatasourceName());
    }

    private Path dataPath(DataState dataState) {
        if ( stateArea == null )
            return null;
        Path dataPath = stateArea(dataState).resolve(FN.DATA);
        return dataPath;
    }

    /**
     * Return the zone-managed dataset (if any).
     * Return null if the dataset is externally managed.
     */
    public DatasetGraph getDataset(DataState dataState) {
        DatasetGraph dsg = datasets.get(dataState.getDataSourceId());
        if ( dsg != null )
            return dsg;
        return external.get(dataState.getDataSourceId());
    }

    /** Create a dataset appropriate to the storage type.
     * This does <em>not</em> write the configuration details into the on-disk zone information.
     */
    public DatasetGraph localStorage(LocalStorageType storage, Path dataPath) {
        switch(storage) {
            case EXTERNAL:     return null;
            case MEM:          return DatasetGraphFactory.createTxnMem();
            case TDB:
                return TDBFactory.createDatasetGraph(IOX.asLocation(dataPath));
            case TDB2:
                return DatabaseMgr.connectDatasetGraph(dataPath.toString());
            default :
                throw new NotImplemented("Zone::localStorage = "+storage);
        }
    }

    /** Supply a dataset for matching to an attached external data source */
    public void externalStorage(Id datasourceId, DatasetGraph dsg) {
        if ( datasets.containsKey(datasourceId) )
            throw new DeltaConfigException("Data source already regsitered as zone-managed: "+datasourceId);
        if ( external.containsKey(datasourceId) ) {
            DatasetGraph dsg1 = external.get(datasourceId);
            if ( dsg1 == dsg ) {
                LOG.warn("Data source already setup as external; same dataset: "+datasourceId);
                return;
            } else {
                LOG.warn("Data source already setup as external; replacing dataset: "+datasourceId);
            }
        }
        external.put(datasourceId, dsg);
    }

    /** Delete a {@code DataState}. Do not use the {@code DataState} again.
     * This operation makes the state on disk inacessible on reboot.
     * (It may delete the old contents.)
     */
    public void delete(Id dsRef) {
        synchronized (zoneLock) {
            DataState dataState = get(dsRef);
            states.remove(dataState.getDataSourceId());
            datasets.remove(dataState.getDataSourceId());
            external.remove(dataState.getDataSourceId());
            if ( stateArea != null ) {
                Path path = stateArea.resolve(dataState.getDatasourceName());
                if ( true ) {
                    // Really delete.
                    IOX.deleteAll(path);
                } else {
                    // Move aside.
                    Path path2 = IOX.uniqueDerivedPath(path, (x)->x+DELETE_MARKER);
                    try { Files.move(path, path2, StandardCopyOption.ATOMIC_MOVE); }
                    catch (IOException e) { throw IOX.exception(e); }
                }
            }
        }
    }

    // "release" removes from the active zone but leaves untouched on disk.

    /** Release a {@code DataState}. Do not use the {@code DataState} again. */
    public void release(DataState dataState) {
        release(dataState.getDataSourceId());
    }

    /** Release a {@code DataState} by {@code Id}. Do not use the associated {@code DataState} again. */
    public void release(Id dsRef) {
        DataState dataState = states.remove(dsRef);
        if ( dataState == null )
            return ;
        datasets.remove(dsRef);
        external.remove(dsRef);
        names.remove(dataState.getDatasourceName());
    }

    /** Refresh the {@link DataState} of a datasource */
    public void refresh(Id datasourceId) {
        DataState ds = connect(datasourceId);
        if ( ds == null )
            return;
        ds.refresh();
    }

    /** Connect to a {@code DataSorce} that is already in this zone. */
    public DataState connect(Id datasourceId) {
        if ( ! exists(datasourceId) )
            throw new DeltaConfigException("Not found: "+datasourceId);
        return states.get(datasourceId);
    }

    // Agrees with IOX.uniqueDerivedPath for DELETE_MARKER
    private static Pattern DELETED = Pattern.compile(DELETE_MARKER+"-\\d+$");
    private static boolean isDeleted(Path path) {
        String fn = path.getFileName().toString();
        return DELETED.matcher(fn).find();
    }

    public Path statePath(DataState dataState) {
        if ( stateArea == null )
            return null;
        dataState.getStatePath();
        return stateArea.resolve(dataState.getDatasourceName());
    }

    public DataState get(Id datasourceId) {
        return states.get(datasourceId);
    }

    public Id getIdForName(String name) {
        return names.get(name);
    }


    // "release" removes from the active zone but leaves untouched on disk.

    public Location getLocation() {
        return stateLocation;
    }

    /** Put state file name into DataState then only have here */
    private DataState readDataState(Path p) {
        Path versionFile = p.resolve(FN.STATE);
        if ( ! Files.exists(versionFile) )
            throw new DeltaConfigException("No state file: "+versionFile);

        PersistentState state = new PersistentState(versionFile);
        if ( state.getString().isEmpty() )
            throw new DeltaConfigException("Error reading state: version file exist but is empty");
        DataState dataState = new DataState(this, state) ;
        return dataState;
    }

    /** Scan a directory for DataSources.
     * See {@code LocalServer.scanDirectory} for a similar operation on the server side.
     */
    private static List<Path> scanForDataState(Location workarea) {
        Path dir = IOX.asPath(workarea);
        try {
            List<Path> datasources = Files.list(dir)
                .filter(p->Files.isDirectory(p))
                // Not deleted and moved aside.
                .filter(p->!isDeleted(p))
                .filter(Zone::isFormattedDataState)
                .collect(Collectors.toList());
            return datasources;
        }
        catch (IOException ex) {
            DataState.LOG.error("Exception while reading "+dir);
            throw IOX.exception(ex);
        }
    }

    private static boolean isFormattedDataState(Path path) {
        // Directory: "data/"
        // File: "state"

        boolean good = true;
        Path dataArea = path.resolve(FN.DATA);
//        if ( ! Files.exists(dataArea) ) {
//            // Should check its not a memory area.
//            FmtLog.warn(DataState.LOG,  "No data area: %s", path);
//            good = false;
//            //return false;
//        }

        Path pathState = path.resolve(FN.STATE);
        if ( ! Files.exists(pathState) )  {
            FmtLog.warn(DataState.LOG,  "No state file: %s", path);
            good = false;
        }
        // Development - try to continue.
        return true;
        //return good;
    }

    @Override
    public String toString() {
        return "Zone["+stateLocation+", "+states.keySet()+"]";
    }
}
