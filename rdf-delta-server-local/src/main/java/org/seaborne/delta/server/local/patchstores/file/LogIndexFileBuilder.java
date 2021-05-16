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

package org.seaborne.delta.server.local.patchstores.file;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.Id;
import org.seaborne.delta.Version;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.server.local.LogEntry;
import org.seaborne.delta.server.local.patchstores.filestore.FileStore;
import org.seaborne.patch.PatchHeader;
import org.seaborne.patch.text.RDFPatchReaderText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Build LogIndexFile from a FileStore */
public class LogIndexFileBuilder {
    private static Logger LOG = LoggerFactory.getLogger(LogIndexFile.class);

    /**
     * Inspect a {@link FileStore} and build the in-memory maps.
     * Return the latest version or null if no patches.
     */
    /*package*/ static LogIndexFile initFromFileStore(FileStore fileStore) {
        Map<Version, Id> versionToId = new ConcurrentHashMap<>();
        Map<Id, LogEntry> logEntries = new ConcurrentHashMap<>();

        // Only used locally.
        Map<Id, Version> trackIdToVersion = new HashMap<>();

        // Iterator is sorted.
        Iterator<Long> iter = fileStore.getIndexes().iterator();
        // DEBUG
//        List<Long> x = Iter.toList(iter);
//        System.out.println(x);
//        iter = x.iterator();
        // DEBUG

        Version earliestVersion = null;     // Not found yet.
        Version currentPreviousVersion = null;
        Version currentVersion = null;      // Not found yet.
        PatchHeader previous = null;
        boolean first = true;

        for ( ; iter.hasNext() ; ) {
            long idx = iter.next();
            try ( InputStream in = fileStore.open(idx) ) {
                PatchHeader patchHeader = RDFPatchReaderText.readerHeader(in);
                if ( patchHeader == null ) {
                    FmtLog.error(LOG, "Can't read header: idx=%d", idx);
                    continue;
                }
                Id id = Id.fromNode(patchHeader.getId());
                if ( id == null ) {
                    FmtLog.error(LOG, "Can't find id: idx=%d: id=%s", idx, id);
                    continue;
                }
                else {
                    if ( trackIdToVersion.containsKey(id) ) {
                        FmtLog.error(LOG, "Duplicate: idx=%d: id=%s", idx, id);
                    }
                }

                Id prev = Id.fromNode(patchHeader.getPrevious());
                if ( prev != null ) {
                    // We process entries in order so we should have seen previous by now.
                    if ( ! trackIdToVersion.containsKey(prev) ) {
                        FmtLog.error(LOG, "Can't find previous: idx=%d: id=%s, prev=%s", idx, id, prev);
                        continue;
                    }
                }

                Version ver = Version.create(idx);
                if ( logEntries != null ) {
                    LogEntry patchInfo = new LogEntry(id, ver, prev);
                    logEntries.put(id, patchInfo);
                }
                trackIdToVersion.put(id, ver);
                versionToId.put(ver, id);
                if ( earliestVersion == null )
                    earliestVersion = ver;
                currentPreviousVersion = currentVersion;
                currentVersion = ver;
            }
            catch (NoSuchFileException ex) { throw IOX.exception(ex); }
            catch (IOException ex)  { throw IOX.exception(ex); }
        }
        return new LogIndexFile(fileStore, versionToId, currentVersion, currentPreviousVersion, earliestVersion, logEntries);
    }
}
