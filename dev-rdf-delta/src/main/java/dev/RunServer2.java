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

import java.io.File;
import java.io.IOException;
import java.io.InputStream ;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.fuseki.build.DatasetDescriptionRegistry;
import org.apache.jena.fuseki.server.FusekiServer;
import org.apache.jena.tdb.base.file.Location ;
import org.seaborne.delta.server.* ;
import org.seaborne.delta.server.http.DataPatchServer ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunServer2 {
    static { LogCtl.setJavaLogging(); }
    private static Logger LOG = LoggerFactory.getLogger("Main") ; 
    
    public static void main(String ...args) {
        try { mainMain(); }
        catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
        System.out.println("DONE");
        System.exit(0) ;
    }
     
    public static void mainMain() {
        // Zone -> DataRegistry 
        // DataRegistry -> DataSource
        // DataSource = one changing 
        
        
        String SOURCES = "/home/afs/ASF/rdf-delta/Sources" ;
        
        Id id = Id.fromUUID(C.uuid1) ;
        
        // Setup - need better registration based on scan-find.
        List<String> sourceAreas = /*FileOps.*/dirEntries(SOURCES);
        
        
        Location sourceArea = Location.create(SOURCES) ;
        DataSource source = DataSource.build(sourceArea) ;
        System.out.println(source) ;
        DataRegistry dReg = DataRegistry.get();
        dReg.put(source.getId(), source);

        // Server.
        DataPatchServer dps = new DataPatchServer(4040) ;
        dps.start(); 
        
        // Configure the patch pool listeners.
        
        //---- Test data.
        //Patch id=uuid:f5fdbad2-99f7-11e6-b769-bbc6688fff6c (parent=null)
        Id patch1 = Id.fromString("f5fdbad2-99f7-11e6-b769-bbc6688fff6c") ;
        processPatch(id, "data1.rdfp");
        // Patch id=uuid:0577ce6c-99f8-11e6-8a94-43918cb86532 (parent=uuid:f5fdbad2-99f7-11e6-b769-bbc6688fff6c)
        Id patch2 = Id.fromString("0577ce6c-99f8-11e6-8a94-43918cb86532") ;
        processPatch(id, "data2.rdfp");
        
        // --- check
        DataSource dataSource = DataRegistry.get().get(id) ;
        PatchSet ps = dataSource.getPatchSet() ;
        ps.getInfo() ;
        
        //
        System.out.println("Check") ;
        ps.processHistoryFrom(null, (p)->System.out.println(p.getId()) );
        
        DataRegistry.get().forEach((srcId,ds)->{
            if ( ds.getPatchSet().contains(patch1) )
                System.out.println("Found!"); ;
        });
        
        // Name datasource or not?
        API.fetch(id, patch1) ;
        
    }

    private static List<String> dirEntries(String directory) {
        Path pDir = Paths.get(directory).normalize() ;
        File dirFile = pDir.toFile() ;
        
        if ( ! dirFile.exists() )
            return Collections.emptyList() ;
        
        if ( ! dirFile.isDirectory() ) {
            LOG.warn("Not a directory: '"+directory+"'") ;
            return Collections.emptyList() ;
        }
        // Files that are not hidden.
        DirectoryStream.Filter<Path> directoryFilter = (entry)-> {
            File f = entry.toFile() ;
            return ! f.isHidden() && f.isDirectory() ;
        } ;

        List<String> entries = new ArrayList<>() ;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pDir, directoryFilter)) {
            for ( Path p : stream ) {
                DatasetDescriptionRegistry dsDescMap = FusekiServer.registryForBuild() ;
                LOG.info("Dir: "+p.toString()) ; 
            }
        } catch (IOException ex) {
            LOG.warn("IOException:"+ex.getMessage(), ex);
        }
        return entries ;
    }
    
    // See DPS.
    
    // Area management.
    
    // Choose a name.
    private static String nextEntry(Path dir, String base) {
        return null ;
        //dir.resolve(base)
    }

    private static void processPatch(Id id, String filename) {
        InputStream in = IO.openFile(filename) ;
        API.receive(id, in) ;
    }


        
}
