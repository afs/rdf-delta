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

import static org.seaborne.delta.DeltaConst.DATA;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.*;
import org.seaborne.delta.client.DeltaConnection.Backing;
import org.seaborne.delta.lib.IOX;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A "Zone" is a collection of named data sources. */
public class Zone {
    private static Logger LOG = LoggerFactory.getLogger(Zone.class);

    // Zone state
    private volatile boolean INITIALIZED = false;
    private Map<Id, DataState> states = new ConcurrentHashMap<>();
    private Path connectionStateArea = null;
    private Object zoneLock = new Object();
    
    //XXX Current Restriction : one zone. 
    private Zone() {}
    
    private static Zone singleton = new Zone();
    
    public static Zone get() {
        return singleton;
    }
    
    public void reset() {
        states.clear();
    }
    
    /** Reset to the uninitialized state., Shodul not bne needed in normal operation 
     * mainly for testing. */
    public void shutdown() {
        synchronized(zoneLock) {
            if ( ! INITIALIZED )
                return ;
            states.clear();
            connectionStateArea = null;
            INITIALIZED = false;
        }
    }
    
    public List<Id> localConnections() {
        return new ArrayList<>(states.keySet());
    }
    
    public void init(Location area) {
        if ( INITIALIZED ) {
            checkInit(area);
            return;
        }
        synchronized(zoneLock) {
            if ( INITIALIZED ) {
                checkInit(area);
                return;
            }
            INITIALIZED = true;
            connectionStateArea = IOX.asPath(area);
            List<Path> x = scanForDataState(area);
            x.forEach(p->LOG.info("Connection : "+p));
            x.forEach(p->{
                DataState dataState = readDataState(p);
                states.put(dataState.getDataSourceId(), dataState);
            });
        }
    }
    
    private boolean isInitialized() {
        return INITIALIZED;
    }

    private void checkInit(Location area) {
        if ( ! connectionStateArea.equals(area) )
            throw new DeltaException("Attempt to reinitialize the Zone: "+connectionStateArea+" => "+area);
    }

    /** Is there an area aready? */
    public boolean exists(Id dsRef) {
        return states.containsKey(dsRef);
    }

    /** Initialize a new area. */
    public DataState create(String name, Id dsRef, Backing backing) {
        synchronized (zoneLock) {
            if ( states.containsKey(dsRef) )
                throw new DeltaException("Already exists: data state for " + dsRef + " : name=" + name);
            Path conn = connectionStateArea.resolve(name);
            FileOps.ensureDir(conn.toString());
            Path statePath = conn.resolve(DataState.STATE_FILE);
            // XXX PathOps.
            Path dataPath = conn.resolve(DATA);
            FileOps.ensureDir(dataPath.toString());

            // Write disk.
            DataState dataState = new DataState(this, statePath, dsRef, 0);
            states.put(dsRef, dataState);

            // switch (backing) {
            // case TDB:
            // case FILE:
            //
            // default:
            // throw new InternalErrorException("Unknow backing storage type: "+backing);
            //
            // }
            return dataState;
        }
    }
    
    /** Refresh the DataState of a datasource */  
    public void refresh(Id datasourceId) {
        DataState ds = attach(datasourceId);
        if ( ds == null )
            return;
        ds.refresh();
    }

    public DataState attach(Id datasourceId) {
        if ( ! exists(datasourceId) )
            throw new DeltaConfigException("Not found: "+datasourceId);
        return states.get(datasourceId);
    }

    /** Release a {@code DataState}. Do not use the {@code DataState} again. */ 
    public void release(DataState dataState) {
        release(dataState.getDataSourceId());
    }
    
    /** Release by {@Id DataState}. Do not use the associated {@code DataState} again. */ 
    public void release(Id dsRef) {
        states.remove(dsRef);
    }

    /** Put state file name into DataState then only have here */  
    private DataState readDataState(Path p) {
        Path versionFile = p.resolve(DataState.STATE_FILE);
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
        Path dataArea = path.resolve(DeltaConst.DATA);
        if ( ! Files.exists(dataArea) ) {
            FmtLog.warn(DataState.LOG,  "No data area: %s", path);
            good = false;
            //return false;
        }
        
        Path pathState = path.resolve(DataState.STATE_FILE);
        if ( ! Files.exists(pathState) )  {
            FmtLog.warn(DataState.LOG,  "No state file: %s", path);
            good = false;
        }
        // Development - try to continue.
        return true;
        //return good;
    }
}
