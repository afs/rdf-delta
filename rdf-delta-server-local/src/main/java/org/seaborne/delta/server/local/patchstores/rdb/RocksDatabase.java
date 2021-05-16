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

import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;

import org.rocksdb.*;
import org.seaborne.delta.DeltaException;

public class RocksDatabase {

    static { RocksDB.loadLibrary(); }

    //   RocksDB: index:
    //     (version, id)
    //     (id, PatchInfo)  ?? for PatchLogIndex.getPatchInfo(Id)
    //   RocksDB: patch storage
    //     (id, patch)

    Map<String, ColumnFamilyHandle> columnFamilyHandles = new HashMap<>();

    private List<AutoCloseable> closeables = new ArrayList<>();
    private final Path path;
    //May not need a TransactionDB
    // [TransactionDB]
    private RocksDB rocksDB;
    private boolean closed = false;

    public RocksDatabase(Path database) {
        Objects.requireNonNull(database, "database");
        path = database;
        try {
            ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
            cfOpts.optimizeUniversalStyleCompaction();
            closeables.add(cfOpts);

            // list of column family descriptors, first entry must always be default column family
            List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                new ColumnFamilyDescriptor(RocksConst.B_CF_VERSION_ID, cfOpts),
                new ColumnFamilyDescriptor(RocksConst.B_CF_ID_ENTRY, cfOpts),
                new ColumnFamilyDescriptor(RocksConst.B_CF_PATCH, cfOpts)
                );

            // a list which will hold the handles for the column families once the db is opened

            DBOptions dbOptions = new DBOptions();
            dbOptions.setCreateIfMissing(true)
                     .setCreateMissingColumnFamilies(true)
                     .setCompactionReadaheadSize(2*1024*1024);
            closeables.add(dbOptions);

            TransactionDBOptions txnOpt = new TransactionDBOptions();
            // Optimistic transaction policy.
            txnOpt.setWritePolicy(TxnDBWritePolicy.WRITE_UNPREPARED);
            //txnOpt.setWritePolicy(TxnDBWritePolicy.WRITE_COMMITTED)
            closeables.add(txnOpt);

            List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

            // [TransactionDB] -- problems closing.
            //rocksDB = TransactionDB.open(dbOptions, txnOpt, database.toString(),  cfDescriptors, columnFamilyHandleList);
            rocksDB = RocksDB.open(dbOptions, database.toString(),  cfDescriptors, columnFamilyHandleList);
            // For order, see cfDescriptors. 0 is the default column.
            columnFamilyHandles.put(RocksConst.CF_VERSION_ID, columnFamilyHandleList.get(1));
            columnFamilyHandles.put(RocksConst.CF_ID_ENTRY, columnFamilyHandleList.get(2));
            columnFamilyHandles.put(RocksConst.CF_PATCH, columnFamilyHandleList.get(3));

            closeables.add(rocksDB);

        } catch (RocksDBException e) {
            throw new DeltaException("Exception while setting up RocksDB at "+database ,e);
        }
    }

    /** Return the RocksDB database handle. */
    public Path getPath() {
        return path;
    }

    /** Return the RocksDB database handle. */
    public RocksDB getDatabase() {
        return rocksDB;
    }

    /** Must close */
    public RocksIterator iterator(String columnFamily) {
        ColumnFamilyHandle cfh = getColumnFamilyHandle(columnFamily);
        return rocksDB.newIterator(cfh);
    }

    public void iterator(String columnFamily, Consumer<RocksIterator> action) {
        ColumnFamilyHandle cfh = getColumnFamilyHandle(columnFamily);
        try ( RocksIterator iter = rocksDB.newIterator(cfh) ) {
            action.accept(iter);
        }
    }

//    public void txn(Consumer<Transaction> action) {
//        try ( WriteOptions wOpt = new WriteOptions() ) {
//            try ( Transaction txn = rocksDB.beginTransaction(wOpt) ) {
//                action.accept(txn);
//            }
//        }
//    }

    public void update(Consumer<WriteBatch> action) {
        try ( WriteBatch batch = new WriteBatch() ) {
            try ( WriteOptions wOpt = new WriteOptions() ) {
                action.accept(batch);
                rocksDB.write(wOpt, batch);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void close() {
        if ( closed )
            return;
        try {
            // [TransactionDB] -- problems closing.
            //txnDB.syncWal();
            rocksDB.close();
            for(int i = closeables.size()-1 ; i >= 0 ; i--) {
                closeables.get(i).close();
            }
            closed = true;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public byte[] get(String columnFamily, byte[] k) {
        try {
            ColumnFamilyHandle cfh = getColumnFamilyHandle(columnFamily);
            return rocksDB.get(cfh, k);
        } catch (RocksDBException e) {
            throw new DeltaException(e);
        }
    }

    public ColumnFamilyHandle getColumnFamilyHandle(String columnFamily) {
        ColumnFamilyHandle cfh = columnFamilyHandles.get(columnFamily);
        if ( cfh == null ) throw new DeltaException("No ColumnFamilyHandle for "+columnFamily);
        return cfh;
    }
}
