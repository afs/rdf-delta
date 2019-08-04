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

import java.util.function.Supplier;

import org.seaborne.delta.Id;
import org.seaborne.delta.LogEntry;
import org.seaborne.delta.Version;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatchLogIndexFile implements PatchLogIndex {
    private static Logger  LOG = LoggerFactory.getLogger(PatchLogIndexFile.class);

    private final Object lock = new Object();

    private final FilePatchIdx filePatchIdx;
    private Id previousId;
    private Id currentId;
    private Version currentVersion;


    public PatchLogIndexFile(FilePatchIdx filePatchIdx) {
        this.filePatchIdx = filePatchIdx;
        this.currentId = filePatchIdx.getCurrentId();
        this.currentVersion = filePatchIdx.getCurrentVersion();
    }

    @Override
    public boolean isEmpty() {
        Version version = filePatchIdx.getCurrentVersion();
        return Version.UNSET.equals(version) || Version.INIT.equals(version);
    }

    @Override
    public Version nextVersion() {
        long version = filePatchIdx.getCurrentVersion().value();
        if ( version < 0 )
            version = 0;
        Version ver = Version.create(version+1);
        return ver;
    }

    @Override
    public void save(Version version, Id patch, Id prev) {
        filePatchIdx.setCurrentLatest(patch, version, prev);
    }

    @Override
    public Version getEarliestVersion() {
        return filePatchIdx.getEarliestVersion();
    }

    @Override
    public Id getEarliestId() {
        return filePatchIdx.getEarliestId();
    }

    @Override
    public Version getCurrentVersion() {
        return filePatchIdx.getCurrentVersion();
    }

    @Override
    public Id getCurrentId() {
        return filePatchIdx.getCurrentId();
    }

    @Override
    public Id getPreviousId() {
        return filePatchIdx.getPreviousId();
    }

    @Override
    public Id versionToId(Version version) {
        return filePatchIdx.versionToId(version);
    }

    @Override
    public Version idToVersion(Id id) {
        return filePatchIdx.idToVersion(id);
    }

    @Override
    public LogEntry getPatchInfo(Id id) {
        return filePatchIdx.getPatchInfo(id);
    }

    @Override
    public void release() {}

    @Override
    public void delete() {}

    @Override
    public void syncVersionInfo() {}

    @Override
    public void runWithLock(Runnable action) {
        synchronized(lock) {
            action.run();
        }
    }

    @Override
    public <X> X runWithLockRtn(Supplier<X> action) {
        synchronized(lock) {
            return action.get();
        }
    }
}
