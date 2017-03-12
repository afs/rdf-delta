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

import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.tdb.base.file.Location ;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.delta.lib.IOX;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * An item (one dataset and it's associated system resources)
 * under the control of the server.
 * <p>
 * These are managed through the {@link DataRegistry}.
 */
public class DataSource {
    private static Logger LOG = LoggerFactory.getLogger(DataSource.class);

    private final Id       id;
    private final String   uri;
    private final Path     initialData;
    private final Location location;
    // Process that can take an input stream and put a patch safe on storage.
    private final Receiver receiver;
    private final PatchLog patchLog;

    // Duplicates location if not in-memory.
    private final Path path;
    // DataSource short name (final component of path). 
    private final String name;
    
    /** Attach to a datasource file area. 
     * @param sourceArea    {@code Sources}
     * @return DataSource
     */
    public static DataSource connect(DataRegistry dataRegistry, Id dsRef, String uri, String name, Location sourceArea) {
        Location patchesArea = LocalServer.patchArea(sourceArea);
        if ( sourceArea.isMem() && patchesArea.isMem() ) {
            return null ;
        }
        
        if ( sourceArea.isMem() || patchesArea.isMem() ) {
            throw new IllegalArgumentException("Mixed in-memory add persistent: source area = "+sourceArea+" : patch area = "+patchesArea);
        }
        
        Path initialData = LocalServer.initialData(sourceArea)   ;          

        formatSourceArea(sourceArea, patchesArea, initialData);
        PatchLog patchSet = loadPatchLog(dsRef, name, patchesArea);
        Receiver receiver = new Receiver(patchSet.getFileStore());
        
        return new DataSource(dsRef, sourceArea, initialData, name, uri, patchSet, receiver);
    }

    private static PatchLog loadPatchLog(Id dsRef, String name, Location patchesArea) {
        return PatchLog.attach(dsRef, name, patchesArea);
    }

    private DataSource(Id id, Location location, Path initialData, String name, String uri, PatchLog patchLog, Receiver receiver) {
        super();
        this.id = id;
        this.location = location;
        this.path = location.isMem() ? null : IOX.asPath(location);
        this.initialData = initialData;  
        this.receiver = receiver;
        this.name = name;
        this.uri = uri;
        this.patchLog = patchLog;
    }

    public Id getId() {
        return id;
    }

    public String getURI() {
        return uri;
    }

    public Receiver getReceiver() {
        return receiver;
    }

    public Location getLocation() {
        return location;
    }

    public Path getInitialDataPath() {
        return initialData;
    }

    /** Get path to file area - returns null if this is an in-memory DataSource. */
    public Path getPath() {
        return path;
    }

    public String getName() {
        return name;
    }

    public PatchLog getPatchLog() {
        return patchLog;
    }

    public DataSourceDescription getDescription() {
        return new DataSourceDescription(id, name, uri); 
    }
    
    public boolean inMemory() {
        return location.isMem(); 
    }
    
    private static void formatSourceArea(Location sourcesArea, Location patchesArea, Path initialData) {
        FileOps.ensureDir(sourcesArea.getDirectoryPath());
        FileOps.ensureDir(patchesArea.getDirectoryPath());
        if ( initialData != null )
            FileOps.ensureDir(initialData.toString());
    }

    @Override
    public String toString() {
        return String.format("Source: %s [%s]", id, uri);
    }
}
