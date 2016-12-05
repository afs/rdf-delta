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
    
    private final String name ;
    private final Id id ;
    // Directory of all resources connected to this DataSourtce.
    private final Location location ;
    // Process that can take an input stream and put a patch safe on storage. 
    private final Receiver receiver ;
    
    // Has version stuff
    private final PatchSet patchSet ;
    
    public static DataSource attach(Location sourceArea, Location patchesArea) {
        formatSourceArea(sourceArea);
        InputStream in = IO.openFile(sourceArea.getPath(CONF)) ;
        if ( in == null ) {
            LOG.warn("No source configuration" );
        }
        // id
        // version
        
        JsonObject obj = JSON.parse(in) ;
        //JSON.write(obj);
        //System.out.println();
        String idStr = getStrOrNull(obj, F_ID) ;
        String versionStr = getStrOrNull(obj, F_VERSION) ;
        String nameStr = getStrOrNull(obj, F_NAME) ;
        FmtLog.info(LOG, "DataSource: source=%s, patches=%s", sourceArea, patchesArea);
        FmtLog.info(LOG, "DataSource: id=%s, version=%s, name=%s", idStr, versionStr, nameStr) ; 
        
        Id id = Id.fromString(idStr) ; 
        // Scan for patch files.
        PatchSet patchSet = loadPatchSet(id, patchesArea.getDirectoryPath());
        
        //Where does version come from?
        Receiver receiver = new Receiver(patchSet.getFileStore());
        return new DataSource(id, sourceArea, nameStr, patchSet, receiver) ;
    }

    private static PatchSet loadPatchSet(Id id, String path) {
        return new PatchSet(id, path) ;
    }

    private static String getStrOrNull(JsonObject obj, String field) {
        JsonValue jv = obj.get(field);
        if ( jv == null ) {
            LOG.warn("Field '"+field+"' not found");
            return null;
        }
        if ( jv.isString() )
            return jv.getAsString().value();
        if ( jv.isNumber() )
            return jv.getAsNumber().value().toString();
        LOG.warn("field "+field+" : returning null for the string value");
        return null ;
    }
    
    private DataSource(Id id, Location location, String name, PatchSet patchSet, Receiver receiver) {
        super() ;
        this.id = id ;
        this.location = location ;
        this.receiver = receiver ;
        this.name = name ;
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
