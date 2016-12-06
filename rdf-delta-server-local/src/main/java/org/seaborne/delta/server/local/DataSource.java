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

import java.io.InputStream ;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.json.JSON ;
import org.apache.jena.atlas.json.JsonObject ;
import org.apache.jena.atlas.json.JsonValue;
import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.tdb.base.file.Location ;
import org.seaborne.delta.conn.Id ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.seaborne.delta.DPNames.*;

/** 
 * An item (one dataset and it's associated system resources)
 * under control of the server.
 * 
 * These are manged through the {@link DataRegistry}.
 */
public class DataSource {
    private static Logger LOG = LoggerFactory.getLogger(DataSource.class);

    private static String CONF = "source.cfg" ;
    
    // relationship to the Distributor?
    // Maybe one Distributor per DataSource (manages event flow). 
    
    private final Id id ;
    private final String uri ;
    // Directory of all resources connected to this DataSourtce.
    private final Location location ;
    // Process that can take an input stream and put a patch safe on storage. 
    private final Receiver receiver ;
    // Has version stuff
    private final PatchSet patchSet ;
    
    /** Attach to a datasource file area. 
     * Create if necessary.  
     * @param sourceArea    {@code Sources}
     * @param patchesArea   The global {@code Patches} store.
     * @return DataSource
     */
    public static DataSource attach(Id id, String uri, Location sourceArea, Location patchesArea) {
        formatSourceArea(sourceArea, patchesArea);
        PatchSet patchSet = loadPatchSet(id, patchesArea.getDirectoryPath());
        Receiver receiver = new Receiver(patchSet.getFileStore());
        return new DataSource(id, sourceArea, uri, patchSet, receiver) ;
    }

    private static PatchSet loadPatchSet(Id id, String path) {
        return new PatchSet(id, path) ;
    }

    private DataSource(Id id, Location location, String name, PatchSet patchSet, Receiver receiver) {
        super() ;
        this.id = id ;
        this.location = location ;
        this.receiver = receiver ;
        this.uri = name ;
        this.patchSet = patchSet ;
    }

    public Id getId() {
        return id ;
    }

    public Receiver getReceiver() {
        return receiver ;
    }

    public Location getLocation() {
        return location ;
    }

    public PatchSet getPatchSet() {
        return patchSet ;
    }

    public static void formatSourceArea(Location sourcesArea, Location patchesArea) {
        FileOps.ensureDir(sourcesArea.getDirectoryPath());
        FileOps.ensureDir(patchesArea.getDirectoryPath());
    }

    @Override
    public String toString() {
        return String.format("Source: %s [%s]", id, uri) ; 
    }
    
}
