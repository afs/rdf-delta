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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.conn.DeltaConnection;
import org.seaborne.delta.server.http.DataPatchServer;
import org.slf4j.Logger;

/** Command line run the server. */ 
public class CmdDeltaServer {
    public static void setLogging() {
        //LogCtl.setLog4j();
        LogCtl.setJavaLogging();
    }
    
    static { setLogging(); }
    
    private static Logger LOG = DPS.LOG; 
    
    // Environment variable name for the runtime area for the Delta server.
    public static final String BASE = "DELTA_BASE";
    // Relative path name for the "sources" area.
    public static final String SOURCES = "Sources";
    // Relative path name for the patches area.
    public static final String PATCHES = "Patches";
    
    public static void main(String...args) {
        int port = 4040;
        
        String runtimeArea = System.getenv(BASE);
        if ( runtimeArea == null ) {
            System.err.println("Environment variable DELTA_BASE not set");
            System.exit(1);
        }
        
        Path base = Paths.get(runtimeArea).toAbsolutePath();
        if ( ! Files.exists(base) ) {
            System.err.println("No such directory: "+base);
            System.exit(1);
        }
        if ( ! Files.isDirectory(base) ) {
            System.err.println("Exists, but is not a directory: "+base);
            System.exit(1);
        }
        
        // Setup
        Path sources = base.resolve(SOURCES).toAbsolutePath();
        Path patches = base.resolve(PATCHES).toAbsolutePath();
        FileOps.ensureDir(sources.toString());
        FileOps.ensureDir(patches.toString());
        // Setup - need better registration based on scan-find.
        Location sourceArea = Location.create(sources.toString());
        DataSource source = DataSource.attach(sourceArea) ;

        FmtLog.info(LOG, "Delta Server port=%d, sources=%s", port, sources.toString());
        
        DataRegistry dReg = DataRegistry.get();
        dReg.put(source.getId(), source);

        // Server.
        DeltaConnection impl =  new DeltaConnectionLocal();
        DataPatchServer dps = new DataPatchServer(4040, impl) ;
        // And away we go.
        dps.start(); 
        //dps.join();
    }
}
