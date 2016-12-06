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

import static org.seaborne.delta.DPNames.*;
import static org.seaborne.delta.DPNames.F_PORT;
import static org.seaborne.delta.DPNames.F_SOURCES;
import static org.seaborne.delta.DPNames.F_VERSION;
import static org.seaborne.delta.DPNames.PORT;

import java.io.InputStream;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonValue;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.DPNames;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.conn.Id;
import org.seaborne.delta.lib.J;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A local server */
public class LocalServer {
    private static Logger LOG = LoggerFactory.getLogger(LocalServer.class);

    private final DataRegistry dataRegistry;
    
    /** Attach to the runtime area for the server.
     * @param serverRoot
     * @param confFile  Filename: absolute filename, or relative to the server process.
     * @return LocalServer
     */
    public static LocalServer attach(Location serverRoot, String confFile) {
        // XXX ?? Per server? Or per "zone"?
        DataRegistry dataRegistry = DataRegistry.get();
        
        InputStream in = IO.openFile(confFile);
        if ( in == null ) {
            LOG.warn("No source configuration" );
        }
        
        JsonObject rootObj = JSON.parse(in) ;
        // Top level object:
        // {  "port"    : ...,
        //    "sources" : [ ... ]
        //   "version"  -- unused, must be 1
        // }
        int version = J.getInt(rootObj, F_VERSION, -99);
        if ( version == -99 )
            LOG.warn("No version number for the configruation file : assuming 'current'");
        
        int port = J.getInt(rootObj, F_PORT, -1);
        if ( port <= 0  ) {
            LOG.info("No port number: Using default "+DPNames.PORT);
            port = PORT;
        }
        JsonValue jv = rootObj.get(F_SOURCES);
        if ( ! jv.isArray() )
            throw new DeltaConfigException("Not an array for field \""+F_SOURCES+"\""); 
        JsonArray sources = jv.getAsArray();

        // Sources array of objects:
        // { "basename" -- short name used to find the directory 
        //   "id"       -- the global id.
        //   "name"     -- the URL naming this
        // }

        for ( JsonValue v : sources ) {
            JsonObject sourceObj = v.getAsObject();
        
            String idStr = J.getStrOrNull(sourceObj, F_ID) ;
            Id id = Id.fromString(idStr) ; 
            String uriStr = J.getStrOrNull(sourceObj, F_URI) ;
            String baseStr = J.getStrOrNull(sourceObj, F_BASE);
            Location sourceArea = serverRoot.getSubLocation(baseStr);
            
            if ( ! sourceArea.exists() )
                throw new DeltaConfigException("No such directory: "+sourceArea);
            
            Location patchesArea = sourceArea.getSubLocation(DPNames.PATCHES);
            FmtLog.info(LOG, "DataSource: id=%s, source=%s, patches=%s", id, sourceArea, patchesArea);
            DataSource dataSource = DataSource.attach(id, uriStr, sourceArea, patchesArea);
            FmtLog.info(LOG, "DataSource: %s (%s)", dataSource, baseStr);
            
            DataRegistry.get().put(id, dataSource);
        }
        
        return new LocalServer(dataRegistry);
    }
    
    public LocalServer(DataRegistry dataRegistry) {
        this.dataRegistry = dataRegistry;
    }

    public DataRegistry getDataRegistry() {
        return dataRegistry;
    }
}
