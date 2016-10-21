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

package org.seaborne.delta.server2;

import java.io.InputStream ;
import java.util.UUID ;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.tdb.base.file.Location ;
import org.seaborne.delta.server2.API.Registration ;
import org.seaborne.delta.server2.http.DataPatchServer ;
import org.seaborne.patch.RDFPatch ;

public class RunServer2 {
    static { LogCtl.setJavaLogging(); }
    
    public static void main(String ...args) {
        UUID uuid = UUID.fromString("d2640074-955e-11e6-8d16-db70a4103216") ;
        Id id = Id.fromUUID(C.uuid1) ;
        
        DataPatchServer dps = new DataPatchServer(4040) ;
        dps.start(); 
        
        
        // Setup - need better registration based on scan-find.
        Location sourceArea = Location.create("Sources") ;
        formatSourceArea(sourceArea) ;
        DataRegistry dReg = DataRegistry.get();
        DataSource source = new DataSource(id, sourceArea, null) ;
        dReg.put(source.getId(), source);
        
        // ?? Registration reg = API.register("http://ds/name", source) ;
        
        
        //---- Test data.
        InputStream in = IO.openFile("data.rdfp") ;

        
        
        
        // registration -> channel name.
        
        // About = destination 
        
        API.receive(id, in) ;
        
    }

    private static String PATCHES = "Patches" ;
    private static String CONF = "source.ttl" ;
    
    private static void formatSourceArea(Location sourceArea) {
        // Dev - clean start.
        FileOps.clearAll(sourceArea.getDirectoryPath()) ;
        
        FileOps.ensureDir(sourceArea.getPath(PATCHES));
        // CONF
    }
        
}
