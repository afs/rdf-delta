/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.server.local.patchstores.mem;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.seaborne.delta.Id;
import org.seaborne.delta.Version;
import org.seaborne.delta.server.local.LogEntry;
import org.seaborne.delta.server.local.patchstores.LogIndex;

/** Implementation of {@link LogIndex} in-memory. */
public class LogIndexMem implements LogIndex {

    private Version currentVersion = Version.INIT;
    private AtomicLong currentVersionCounter = new AtomicLong(0);
    private Version earliest = Version.INIT;
    private Map<Version, Id> versionToId = new ConcurrentHashMap<>();
    private Map<Id, LogEntry> entries = new ConcurrentHashMap<>();

    public LogIndexMem() {}

    @Override
    public void save(Version version, Id id, Id previous) {
        entries.put(id, new LogEntry(id, version, previous));
        versionToId.put(version, id);
        currentVersion = version;
        if ( earliest == null )
            earliest = version;
    }

    @Override
    public Stream<LogEntry> entries() {
        return entries.values().stream();
    }

    @Override
    public Id versionToId(Version version) {
        Objects.requireNonNull(version);

        return versionToId.get(version);
    }

    @Override
    public Version genNextVersion() {
        return Version.create(currentVersionCounter.incrementAndGet());
    }

    @Override
    public LogEntry getPatchInfo(Id id) {
        return entries.get(id);
    }

    @Override
    public Version earliest() {
        return earliest;
    }

    @Override
    public Version current() {
        return currentVersion;
    }
}
