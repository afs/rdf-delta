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

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
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
    private final PatchLog patchLog;
    // Optional path to a file area for further information.
    // For the file-backed PatchStore, this includes the patches.
    private final Path dsPath;

    /**
     * Attach to a {@link DataSource} file area and return a {@link DataSource} object.
     * The directory {@code dsPath} must exist.
     * The {@code DataSource} area is not formatted by the provider. 
     */
    public static DataSource connect(DataSourceDescription dsd, PatchStore patchStore, Path dsPath) {
        PatchLog patchLog = patchStore.connectLog(dsd, dsPath);
        DataSource dataSource = new DataSource(dsd, dsPath, patchLog);
        return dataSource;
    }

//    /**
//     * Attach to a data source and return a {@link DataSource} object.
//     * The directory {@code dsPath} must exist.
//     */
//    public static DataSource create(DataSourceDescription dsd, Path dsPath) {
//        // [FILE]
//        PatchStore patchStore = selectPatchStore(dsd.getId());
//        PatchLog patchLog = patchStore.createLog(dsd, dsPath);
//        DataSource dataSource = new DataSource(dsd, dsPath, patchLog);
//        return dataSource;
//    }

    public static DataSource create(DataSourceDescription dsd, Path dsPath, PatchStore patchStore) {
        PatchLog patchLog = patchStore.createLog(dsd, dsPath);
        DataSource dataSource = new DataSource(dsd, dsPath, patchLog);
        return dataSource;
    }

    /**
     * Choose the {@link PatchStore} for creating a new {@link PatchLog}
     * Return the current global default if not specifically found
     */
    private static PatchStore selectPatchStore(Id dsRef) {
//     // Look in existing bindings.
//     PatchStore patchStore = ??? ;
//     if ( patchStore != null )
//         return patchStore;
        return PatchStoreMgr.getDftPatchStore();
    }
    
    private DataSource(DataSourceDescription dsd, Path dsPath, PatchLog patchLog) {
        super();
        this.dsDescription = dsd;
        this.dsPath = dsPath; 
        this.patchLog = patchLog;
    }

    public Id getId() {
        return dsDescription.getId();
    }

    public String getURI() {
        return dsDescription.getUri();
    }

    public String getName() {
        return dsDescription.getName();
    }

    /** Return the optional {@link Path} associated with this {@link DataSource}. */ 
    public Path getPath() {
        return dsPath;
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
    
    public void release() {
        PatchStore.release(getPatchLog());
        if ( dsPath != null ) {
            // [FILE] Move into PatchStoreFile.release.
            Cfg.retire(dsPath);
        }
    }

    @Override
    public String toString() {
        return String.format("[DataSource:%s %s]", dsDescription.getName(), dsDescription.getId());
    }
}
