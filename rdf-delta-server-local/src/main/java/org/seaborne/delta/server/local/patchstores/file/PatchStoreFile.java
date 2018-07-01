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

package org.seaborne.delta.server.local.patchstores.file;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.server.local.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatchStoreFile extends PatchStore {
    private static Logger LOG = LoggerFactory.getLogger(PatchStoreFile.class);
    
    /*   Server Root
     *      delta.cfg
     *      /NAME ... per DataSource.
     *          /source.cfg
     *          /Log -- patch on disk (optional)
     *          /data -- TDB database (optional)
     *          /disabled -- if this file is present, then the datasource is not accessible.  
     */
   
    private Map<DataSourceDescription, PatchLog> logs = new ConcurrentHashMap<>();

    private final Path serverRoot;
    
    public PatchStoreFile(String location, PatchStoreProvider provider) {
        this(Paths.get(location), provider);
    }
    
    public PatchStoreFile(Path location, PatchStoreProvider provider) {
        super(provider);
        IOX.ensureDirectory(location);
        this.serverRoot = location; 
    }

    @Override
    public boolean callInitFromPersistent(LocalServerConfig config) {
        return true;
    }

    @Override
    public List<DataSource> initFromPersistent(LocalServerConfig config) {
        // Patch Logs are directories in the server root directory.
        // This will call to create the logs based on "log_type"
        List<DataSource> dataSources = CfgFile.scanForDataSources(serverRoot, this, LOG);
        return dataSources;
    }

    @Override
    public List<DataSourceDescription> listDataSources() {
        return new ArrayList<>(logs.keySet());
    }

    @Override
    protected PatchLog create(DataSourceDescription dsd, Path dsPath) {
        Path patchLogArea = serverRoot.resolve(dsd.getName());
        if ( ! Files.exists(patchLogArea) ) 
            CfgFile.setupDataSourceByFile(serverRoot, this, dsd);
        Location loc = Location.create(patchLogArea.toString());
        PatchLog pLog = PatchLogFile.attach(dsd, this, loc);
        logs.put(dsd, pLog);
        return pLog;
    }

    @Override
    protected void delete(PatchLog patchLog) {
        logs.remove(patchLog.getDescription());
        Path p = ((PatchLogFile)patchLog).getFileStore().getPath();
        CfgFile.retire(p);
        patchLog.release();
    }

}
