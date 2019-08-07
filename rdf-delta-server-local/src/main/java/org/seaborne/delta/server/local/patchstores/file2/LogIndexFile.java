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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.seaborne.delta.Id;
import org.seaborne.delta.Version;
import org.seaborne.delta.server.local.LogEntry;
import org.seaborne.delta.server.local.filestore.FileStore;
import org.seaborne.delta.server.local.patchstores.LogIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The details of the patch file are for one patch log, constructed by parsing the {@link FileStore}. */
public class LogIndexFile implements LogIndex {
    private static Logger  LOG = LoggerFactory.getLogger(LogIndexFile.class);

    //Use LogIndexMem?

    // For reference.
    private final FileStore fileStore;
    private final Path patchDir;
    private final Map<Version, Id> versionToId;
    private final Map<Id, LogEntry> logEntries;

    // Latest patch at the point of starting in this JVM.
    private Id currentId;
    private Version currentVersion;
    private Id previousId;

    private Id earliestId;
    private Version earliestVersion;
//    private final Id _initialCurrentId;
//    private final Version _initalCurrentVersionId;

    public static LogIndexFile create(FileStore fileStore) {
        LogIndexFile logIndexFile = LogIndexFileBuilder.initFromFileStore(fileStore);
        return logIndexFile;
    }

    /*package*/ LogIndexFile(FileStore fileStore, Map<Version, Id> versionToId,
                             Version latestVersion, Version latestPrevious, Version earliestVersion, Map<Id, LogEntry> logEntries) {
        this.fileStore = fileStore;
        this.patchDir = fileStore.getPath();
        this.versionToId = versionToId;
        this.currentVersion = versionOrDft(latestVersion, Version.INIT);
        this.currentId = versionToId(latestPrevious);
        if ( currentId != null ) {
            LogEntry e = logEntries.get(currentId);
            this.previousId = e.getPrevious();
        } else
            this.previousId = null;
        this.earliestVersion = versionOrDft(earliestVersion, Version.INIT);
        this.earliestId = versionToId(earliestVersion);
        this.logEntries = logEntries;
        // FileStore is not used again except to be carried around for PatchStorageFile.
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

    /*package*/ Version idToVersion(Id id) {
        LogEntry entry = logEntries.get(id);
        if ( entry == null )
            return null;
        return entry.getVersion();
    }

    private Id versionToId(Version version) {
        if ( version == null )
            return null;
        if ( Version.UNSET.equals(version) )
            return null;
        if ( Version.INIT.equals(version) )
            return null;
        return versionToId.get(version);
    }

    @Override
    public void save(Version version, Id id, Id previous) {
        // Does not update FileStore.
        // The update to the PatchStorageFile updates the on-disk recovery state.
        currentId = id;
        currentVersion = version;
        previousId = previous;
        versionToId.put(version, id);
        LogEntry entry = new LogEntry(id, version, previous);
        logEntries.put(id, entry);
        if ( earliestId == null ) {
            earliestId = id;
            earliestVersion = version;
        }
    }

    @Override
    public Stream<LogEntry> entries() {
        List<LogEntry> x = new ArrayList<>(logEntries.values());
        return x.stream();
    }

    @Override
    public Id fetchVersionToId(Version version) {
        return versionToId(version);
    }

    @Override
    public Version genNextVersion() {
        return currentVersion.inc();
    }

    @Override
    public LogEntry fetchPatchInfo(Id id) {
        return null;
    }

    @Override
    public Version earliest() {
        return earliestVersion;
    }

    @Override
    public Version current() {
        return currentVersion;
    }
}
