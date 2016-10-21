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

package org.seaborne.delta.server;

import java.io.InputStream ;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.json.JSON ;
import org.apache.jena.atlas.json.JsonObject ;
import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.tdb.base.file.Location ;

/** 
 * An item (one dataset and it's associated system resources)
 * under control of the server.
 * 
 * These are manged throught he {@link DataRegistry}.
 */
public class DataSource {
    private static String PATCHES = "Patches" ;
    private static String CONF = "source.cfg" ;
    
    private static String F_ID = "id" ;
    private static String F_VERSION = "version" ;
    private static String F_NAME = "name" ;
    
    // relationship to the Distributor?
    // Maybe one Distributor per DataSource (manages event flow). 
    
    private final String name ;
    private final Id id ;
    // Directory of all resources connected to this DataSourtce.
    private final Location location ;
    
    // Has version stuff
    private final PatchSet patchSet ;
    
    public static DataSource build(Location sourceArea) {
        formatSourceArea(sourceArea);
        InputStream in = IO.openFile(sourceArea.getPath(CONF)) ;
        if ( in == null ) {
        }
        // id
        // version
        
        JsonObject obj = JSON.parse(in) ;
        String idStr = getStrOrNull(obj, F_ID) ;
        String versionStr = getStrOrNull(obj, F_VERSION) ;
        String nameStr = getStrOrNull(obj, F_NAME) ;
        //if ( )
        
        Id id = Id.fromString(idStr) ; 
        
        PatchSet patchSet = loadPatchSet(sourceArea.getPath(PATCHES)) ; 
        
        return new DataSource(id, sourceArea, nameStr, patchSet) ;
        
    }

    private static PatchSet loadPatchSet(String path) {
        System.err.println("DataSource.loadPatchSet : No persistence"); 
        return new PatchSet(null) ;
    }

    private static String getStrOrNull(JsonObject obj, String field) {
        try {
            return obj.get(field).getAsString().toString() ;
        } catch (Exception ex) {
            return null ;
        }
    }
    
    private DataSource(Id id, Location location, String name, PatchSet patchSet) {
        super() ;
        this.id = id ;
        this.location = location ;
        this.name = name ;
        this.patchSet = patchSet ;
    }

    public Id getId() {
        return id ;
    }

    public Location getLocation() {
        return location ;
    }

    public PatchSet getPatchSet() {
        return patchSet ;
    }

    private static void formatSourceArea(Location sourceArea) {
        // Dev - clean start.
        // FileOps.clearAll(sourceArea.getDirectoryPath()) ;
        FileOps.ensureDir(sourceArea.getPath(PATCHES));
    }

    @Override
    public String toString() {
        return String.format("Source: %s [%s]", id, name) ; 
    }
    
}
