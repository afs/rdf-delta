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

package org.seaborne.delta.server.local.patchstores.rdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.Id;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.rdfpatch.RDFPatchOps;

/**
 * Patch store in-memory, nothing persisted.
 */
public class PatchStorageRocks implements PatchStorage {

    private final RocksDatabase rdb;
    private final ColumnFamilyHandle handle;

    public PatchStorageRocks(RocksDatabase rdb) {
        this.rdb = Objects.requireNonNull(rdb);
        this.handle = rdb.getColumnFamilyHandle(RocksConst.CF_PATCH);
    }

    @Override
    public Stream<Id> find() {
        List<Id> ids = new ArrayList<>();
        try( RocksIterator iter = rdb.iterator(RocksConst.CF_PATCH) ) {
            iter.seekToFirst();
            while(iter.isValid()) {
                byte[] k = iter.key();
                Id id = Id.fromBytes(k);
                ids.add(id);
                iter.next();
            }
        }
        return ids.stream();
    }

    @Override
    public void store(Id id, RDFPatch value) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(1024*1024);
        RDFPatchOps.writeBinary(out, value);
        byte[] key = id.asBytes();
        byte[] data = out.toByteArray();
        rdb.update(batch-> {
            try {
                batch.put(handle, key, data);
            } catch (RocksDBException ex) {
               throw new DeltaException("Exception writing patch", ex);
            }
        });
    }

    @Override
    public RDFPatch fetch(Id id) {
        byte[] key = id.asBytes();
        byte[] value = rdb.get(RocksConst.CF_PATCH, key);
        if ( value == null )
            return null;
        InputStream in = new ByteArrayInputStream(value);
        RDFPatch patch = RDFPatchOps.readBinary(in);
        return patch;
    }

    @Override
    public void delete(Id id) {
        byte[] key = id.asBytes();
        rdb.update(batch-> {
            try {
                batch.delete(handle, key);
            } catch (RocksDBException ex) {
               throw new DeltaException("Exception writing patch", ex);
            }
        });
    }

    @Override
    public void release() {
        rdb.close();
    }
}
