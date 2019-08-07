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

package org.seaborne.delta.server.local.patchstores.file3;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.lib.Bytes;
import org.apache.jena.atlas.lib.Pair;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.Id;
import org.seaborne.delta.Version;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.server.local.JsonLogEntry;
import org.seaborne.delta.server.local.LogEntry;
import org.seaborne.delta.server.local.patchstores.LogIndex;

public class LogIndexRocks implements LogIndex/* XXX add close()*/ {

    //   RocksDB: index:
    //     (version, id)
    //     (id, PatchInfo)  ?? for PatchLogIndex.getPatchInfo(Id)
    //   RocksDB: patch storage
    //     (id, patch)

    // Assumes calling code (PatchLogIndexBase) makes thread safe calls.
    private final RocksDatabase rdb;
    private Version current;
    private Version earliest = null;

    private final ColumnFamilyHandle cfh_idToLogEntry;
    private final ColumnFamilyHandle cfh_versionToId;

    public LogIndexRocks(RocksDatabase database) {
        rdb = requireNonNull(database);
        cfh_idToLogEntry = rdb.columnFamilyHandles.get(RocksConst.CF_ID_ENTRY);
        cfh_versionToId = rdb.columnFamilyHandles.get(RocksConst.CF_VERSION_ID);
        long ver = findLatest(rdb);
        long verFirst = findEarliest(rdb);

        current = (ver > 0 ) ? Version.create(ver) : Version.INIT;
        earliest = verFirst > 0 ? Version.create(verFirst) : Version.INIT;
    }

    @Override
    public void save(Version version, Id id, Id previous) {
        LogEntry entry = new LogEntry(id, version, previous);
        putLogEntry(rdb, entry);
        if ( earliest == null )
            earliest = version;
        current = version;
    }

    @Override
    public Stream<LogEntry> entries() {
        List<LogEntry> entries = new ArrayList<>();
        try( RocksIterator iter = rdb.iterator(RocksConst.CF_ID_ENTRY) ) {
            iter.seekToFirst();
            while(iter.isValid()) {
                // e = getEntry(long)
                byte[] k = iter.key();
                Id id = Id.fromBytes(k);
                byte[] v = iter.value();
                JsonObject obj = JSONX.fromBytes(v);
                LogEntry e = JsonLogEntry.jsonToLogEntry(obj);
                entries.add(e);
                iter.next();
            }
        }
        return entries.stream();
    }

    public Stream<Pair<Version, Id>> versions() {
        List<Pair<Version, Id>> rows = new ArrayList<>();
        try( RocksIterator iter = rdb.iterator(RocksConst.CF_VERSION_ID) ) {
            iter.seekToFirst();
            while(iter.isValid()) {
                byte[] k = iter.key();
                long ver = Bytes.getLong(k);
                byte[] v = iter.value();
                Id id = Id.fromBytes(v);
                rows.add(Pair.create(Version.create(ver), id));
                iter.next();
            }
        }
        return rows.stream();
    }

    @Override
    public Id fetchVersionToId(Version version) {
        if ( ! version.isValid() )
            return null;
        return versionToId(rdb, version.value());
    }

    @Override
    public Version genNextVersion() {
        if ( current == null )
            return Version.FIRST;
        return current.inc();
    }

    @Override
    public LogEntry fetchPatchInfo(Id id) {
        requireNonNull(id);
        return getLogEntry(rdb, id);
    }

    @Override
    public Version earliest() {
        return earliest;
    }

    @Override
    public Version current() {
        return current;
    }

    public void close() {
        rdb.close();
    }

    private static long findLatest(RocksDatabase rdb) {
        byte[] ver = findEnd(rdb, RocksConst.CF_VERSION_ID, true);
        if ( ver == null )
            return -1;
        return Bytes.getLong(ver);
    }

    private static long findEarliest(RocksDatabase rdb) {
        byte[] ver = findEnd(rdb, RocksConst.CF_VERSION_ID, false);
        if ( ver == null )
            return -1;
        return Bytes.getLong(ver);
    }

    private static byte[] findEnd(RocksDatabase rdb, String cfName, boolean highest) {
        try( RocksIterator iter = rdb.iterator(RocksConst.CF_VERSION_ID) ) {
            if ( highest )
                iter.seekToLast();
            else
                iter.seekToFirst();
            if ( iter.isValid() )
                return iter.key();
            return null;
        }
    }

    private static Id versionToId(RocksDatabase rdb, long latest) {
        byte[] k = Bytes.packLong(latest);
        byte[] v = rdb.get(RocksConst.CF_VERSION_ID, k);
        if ( v == null )
            return null;
        Id id = Id.fromBytes(v);
        return id;
    }

    private static LogEntry getLogEntry(RocksDatabase rdb, Id id) {
        byte[] k = id.asBytes();
        byte[] v = rdb.get(RocksConst.CF_ID_ENTRY, k);
        JsonObject obj = JSONX.fromBytes(v);
        LogEntry e = JsonLogEntry.jsonToLogEntry(obj);
        return e;
    }

    private static void putLogEntry(RocksDatabase rdb, LogEntry entry) {
        byte[] kVer = new byte[Long.BYTES];
        Bytes.setLong(entry.getVersion().value(), kVer);

        ColumnFamilyHandle cfh_idToLogEntry = rdb.columnFamilyHandles.get(RocksConst.CF_ID_ENTRY);
        ColumnFamilyHandle cfh_versionToId = rdb.columnFamilyHandles.get(RocksConst.CF_VERSION_ID);

        byte[] idBytes = entry.getPatchId().asBytes();
        JsonObject obj = JsonLogEntry.logEntryToJson(entry);
        byte[] value = JSONX.asBytes(obj);
        rdb.update(wb->{
            try {
                wb.put(cfh_idToLogEntry, idBytes, value);
                wb.put(cfh_versionToId, kVer, idBytes);
            } catch (RocksDBException ex) { throw new DeltaException(ex); }
        });
    }
}