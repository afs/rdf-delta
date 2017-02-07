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

package org.seaborne.delta.client;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.jena.atlas.lib.NotImplemented;
import org.apache.jena.sparql.core.DatasetGraph;
import org.seaborne.delta.*;
import org.slf4j.Logger;

/** State of a  DeltaConnection.
 *  <br/>{@link DatasetGraph} for client shadow.
 *  <br/>{@link RefLong} for the version number. 
 *  If a memory-connection, then the  
 */
public class DataSourceClient {
   
    // XXX DeltaConnectionState.
    
    private static Logger LOG = Delta.DELTA_LOG;
    private static String versionFile = DPConst.VERSION;
    // Either a DB database or a file "data/daat.ttl".
    private static String dataDiectory = DPConst.DATA;
    
    private final DatasetGraph dsg;
    private final RefLong counter;
    
    public static DataSourceClient attach(String filearea) {
        Path path = Paths.get(filearea);
        if ( ! Files.exists(path) ) {
            LOG.error("No such directory: "+filearea);
            throw new DeltaException("No such directory: "+filearea);
        }
        if ( ! Files.isDirectory(path) ) {
            LOG.error("Not a directory: "+filearea);
            throw new DeltaException("Not a directory: "+filearea);
        }            
        
        Path statePath = path.resolve(versionFile);
        RefLong version = new PersistentState(statePath);
        
        Path dataPath = path.resolve(versionFile);
        if ( Files.exists(dataPath) ) {}
        
        throw new NotImplemented();
        //return new DataSourceClient(null, null);
    }
    
    public static DataSourceClient setup() {
        RefLong version;

        
        throw new NotImplemented();
    }
    
    private DataSourceClient(DatasetGraph dsg, RefLong counter) {
        this.dsg = dsg;
        this.counter = counter;
    }
}
