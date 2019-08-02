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

package org.seaborne.delta.server.local.patchstores.file2;

import java.nio.file.Path;
import java.util.function.Function;

import org.apache.jena.ext.com.google.common.collect.BiMap;
import org.seaborne.delta.Id;
import org.seaborne.delta.Version;
import org.seaborne.delta.server.local.filestore.FileStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The details of the patch file are for one patch log, constructed by parsing the {@link FileStore}. */
public class FilePatchIdx {
    private static Logger  LOG = LoggerFactory.getLogger(FilePatchIdx.class);

    // For reference.
    private final FileStore fileStore;
    private final Path patchDir;
    private final BiMap<Id, Version> idToVersion;
    private final Function<Id, Version> id2version;

    // Latest patch at the point of starting in this JVM.
    private Id currentId;
    private Version currentVersion;
    private Id previousId;

    private Id earliestId;
    private Version earliestVersion;
//    private final Id _initialCurrentId;
//    private final Version _initalCurrentVersionId;

    public static FilePatchIdx create(FileStore fileStore) {
        FilePatchIdx filePatchIdx = FS2.initFromFileStore(fileStore);
        return filePatchIdx;
    }

    /*package*/ FilePatchIdx(FileStore fileStore, BiMap<Id, Version> idToVersion,
                             Version latestVersion, Version latestPrevious, Version earliestVersion) {
        this.fileStore = fileStore;
        this.patchDir = fileStore.getPath();
        this.idToVersion = idToVersion;
        this.id2version = idToVersion::get;
        this.currentVersion = versionOrDft(latestVersion, Version.INIT);
        this.previousId = versionToId(latestPrevious);

//        // Initial starting runtime state.
//        this.initialCurrentId = this.currentId;
//        this.initalCurrentVersionId = this.currentVersion;

        this.earliestVersion = versionOrDft(earliestVersion, Version.INIT);
        this.earliestId = versionToId(earliestVersion);
    }

    private Version versionOrDft(Version ver, Version verDefault) {
        return ( ver == null ) ? verDefault : ver ;
    }

    public FileStore fileStore() {
        return fileStore;
    }

    public Path getPath() {
        return patchDir;
    }

    public Version idToVersion(Id id) {
        return idToVersion.get(id);
    }

    public Id versionToId(Version version) {
        if ( Version.UNSET.equals(version) )
            return null;
        if ( Version.INIT.equals(version) )
            return null;
        return idToVersion.inverse().get(version);
    }

    /*package*/ void setCurrentLatest(Id id, Version version, Id previous) {
        // [FILE2] Does not update FileStore.
        currentId = id;
        currentVersion = version;
        previousId = previous;
        idToVersion.put(id, version);
        if ( earliestId == null ) {
            earliestId = id;
            earliestVersion = version;
        }
    }

    public Id getCurrentId() {
        return currentId;
    }

    public Id getPreviousId() {
        return previousId;
    }

    public Version getCurrentVersion() {
        return currentVersion;
    }

    public Id getEarliestId() {
        return earliestId;
    }

    public Version getEarliestVersion() {
        return earliestVersion;
    }
}
