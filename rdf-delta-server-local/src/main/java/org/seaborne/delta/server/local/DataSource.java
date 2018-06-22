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

import java.nio.file.Path;

import org.apache.jena.tdb.base.file.Location ;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaConst ;
import org.seaborne.delta.Id;
import org.seaborne.delta.lib.IOX ;
import org.seaborne.delta.server.local.patchlog.PatchLog ;
import org.seaborne.delta.server.local.patchlog.PatchStore ;
import org.seaborne.delta.server.local.patchlog.PatchStoreMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * An item under the control of the server.
 * <p>
 * These are managed through the {@link DataRegistry}.
 */
public class DataSource {
    private static Logger LOG = LoggerFactory.getLogger(DataSource.class);
    private final DataSourceDescription dsDescription;
    private final Path     initialData;
    private final PatchLog patchLog;

    // Duplicates location if not in-memory.
    private final Path path;
    
    /**
     * Attach to a {@link DataSource} file area and return a {@link DataSource} object.
     * The directory {@code dsPath} must exist.
     * The {@code DataSource} area is not formatted by the provider. 
     */
    public static DataSource connect(DataSourceDescription dsd, PatchStore patchStore, Path dsPath) {
        PatchLog patchLog = patchStore.connectLog(dsd, dsPath);
        Path initialData = null;
        if ( dsPath != null )
            initialData = dsPath.resolve(DeltaConst.INITIAL_DATA);
        DataSource dataSource = new DataSource(dsd, dsPath, initialData, patchLog);
        return dataSource;
    }

    /**
     * Attach to a datasource file area and return a {@link DataSource} object.
     * The directory {@code dsPath} must exist.
     */
    public static DataSource create(DataSourceDescription dsd, Path dsPath) {
        PatchStore patchStore = PatchStoreMgr.selectPatchStore(dsd.getId());
        PatchLog patchLog = patchStore.createLog(dsd, dsPath);
        // [FILE]
        Path initialData = null;
        if ( dsPath != null )
            initialData = dsPath.resolve(DeltaConst.INITIAL_DATA);
        IOX.ensureFile(initialData);
        DataSource dataSource = new DataSource(dsd, dsPath, initialData, patchLog);
        return dataSource;
    }

    private DataSource(DataSourceDescription dsd, Path location, Path initialData, PatchLog patchLog) {
        super();
        this.dsDescription = dsd;
        this.path = location;
        this.initialData = initialData;  
        this.patchLog = patchLog;
    }

    public Id getId() {
        return dsDescription.getId();
    }

    public String getURI() {
        return dsDescription.getUri();
    }

    public Location getLocation() {
        return Location.create(path.toString());
    }

    public Path getInitialDataPath() {
        return initialData;
    }

    /** Get path to file area - returns null if this is an in-memory DataSource. */
    public Path getPath() {
        return path;
    }

    public String getName() {
        return dsDescription.getName();
    }

    public PatchLog getPatchLog() {
        return patchLog;
    }

    public PatchStore getPatchStore() {
        return patchLog.getPatchStore();
    }

    public DataSourceDescription getDescription() {
        return dsDescription;
    }
    
    public boolean inMemory() {
        return false;
    }
    
    public void release() {
        // Decision what to do with the state?
        //  1 - mark unavailable 
        //  2 - move aside
        //  3 - really delete

        PatchStore.release(getPatchLog());

        if ( ! inMemory() ) {
            Cfg.retire(getPath());
        }
    }

    @Override
    public String toString() {
        return String.format("[DataSource:%s %s]", dsDescription.getName(), dsDescription.getId());
    }
}
