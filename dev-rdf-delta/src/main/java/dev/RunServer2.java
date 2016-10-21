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

package dev;

import java.io.InputStream ;
import java.util.UUID ;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.tdb.base.file.Location ;
import org.seaborne.delta.server.* ;
import org.seaborne.delta.server.API.Registration ;
import org.seaborne.delta.server.http.DataPatchServer ;
import org.seaborne.patch.RDFPatch ;

public class RunServer2 {
    static { LogCtl.setJavaLogging(); }
    
    public static void main(String ...args) {
        try { mainMain(); }
        catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
        System.out.println("DONE");
        System.exit(0) ;
    }
     
    public static void mainMain() {
        UUID uuid = UUID.fromString("d2640074-955e-11e6-8d16-db70a4103216") ;
        Id id = Id.fromUUID(C.uuid1) ;
        
        DataPatchServer dps = new DataPatchServer(4040) ;
        dps.start(); 
        
        
        // Setup - need better registration based on scan-find.
        Location sourceArea = Location.create("Sources") ;
        DataSource source = DataSource.build(sourceArea) ;
        System.out.println(source) ;
        DataRegistry dReg = DataRegistry.get();
        dReg.put(source.getId(), source);
        
        // Configure the patch pool listeners.
        
        //---- Test data.
        InputStream in = IO.openFile("data.rdfp") ;
        // registration -> channel name.
        
        // About = destination 
        
        API.receive(id, in) ;
        
    }


        
}
