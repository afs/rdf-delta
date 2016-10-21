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

import java.io.IOException ;
import java.nio.file.DirectoryStream ;
import java.nio.file.Files ;
import java.nio.file.Path ;
import java.nio.file.Paths ;
import java.util.ArrayList ;
import java.util.Arrays ;
import java.util.List ;
import java.util.Map ;
import java.util.concurrent.ConcurrentHashMap ;

import org.apache.jena.atlas.lib.InternalErrorException ;
import org.apache.jena.atlas.lib.NotImplemented ;
import org.apache.jena.fuseki.server.FusekiServer ;
import org.apache.jena.graph.Node ;
import org.apache.jena.graph.NodeFactory ;
import org.apache.jena.tdb.base.file.Location ;

/** The clients to what they do mapping */ 
public class Datasets {
    
    private static Datasets.Inner singleton = Datasets.Inner.fromDisk() ;
    
    public static DataRef get(Id id) {
        return singleton.get(id) ;
    }

    static class Setup {

        private final String name ;
        private final Location location ;
        private final Node uri ;
        private final Id data ;

        public Setup(String name, Location location, String uristr, Id data) {
            this.name = name ;
            this.location = location ;
            this.uri = NodeFactory.createURI(uristr);
            this.data = data ;
        }
        
    }
    
    static class Inner {

        private Map<Id, DataRef> dataRefs = new ConcurrentHashMap<>() ;
        
        private static final Path DIR = Paths.get("Datasets") ;
        
        // Initialize
        static Inner fromDisk() {
            Inner inner = new Inner() ;
            
            List<String> files = existingConfigurationFiles(DIR, "ds-") ;
            DPS.LOG.info("Dataset files: "+files) ;
            //List<Setup> setups = files.stream().map(Inner::readSetup).collect(Collectors.toList()) ;
            
            List<Setup> setups = dummy() ; 
            
            setups.forEach(s->{
                throw new NotImplemented("DatasetsfromDisk") ;
            } );
            
            return inner ;
        }
            
        private static List<Setup> dummy() {
            Setup setup1 = new Setup("Dataset1", Location.create("DB1"), "http://example/g1", Id.fromUUID(C.uuid1)) ;
            Setup setup2 = new Setup("Dataset2", Location.create("DB2"), "http://example/g2", Id.fromUUID(C.uuid2)) ;      
            return Arrays.asList(setup1, setup2) ;
        }

        static Setup readSetup(String filename) {
            DPS.LOG.info("Setup: "+filename) ;
            return null ;
        }
        
        /** Return the filenames of all matching files in the directory */  
        public static List<String> existingConfigurationFiles(Path directory, String baseFilename) {
            try { 
                List<String> paths = new ArrayList<>() ;
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(DIR, baseFilename+"*") ) {
                    stream.forEach((p)-> paths.add(p.getFileName().toString())) ;
                }
                return paths ;
            } catch (IOException ex) {
                throw new InternalErrorException("Failed to read configuration directory "+FusekiServer.dirConfiguration) ;
            }
        }
            
        
        
        public DataRef get(Id id) {
            return dataRefs.get(id) ;
        }
    }
}
