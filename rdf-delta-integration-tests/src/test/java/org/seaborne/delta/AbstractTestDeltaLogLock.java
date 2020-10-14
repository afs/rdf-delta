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

package org.seaborne.delta;

import static org.apache.jena.atlas.lib.ThreadLib.async;

import java.util.concurrent.Semaphore;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.system.Txn;
import org.junit.Test;
import org.seaborne.delta.client.*;
import org.seaborne.delta.link.DeltaLink;

public abstract class AbstractTestDeltaLogLock {

    abstract protected DeltaLink getDLink();
    protected static Id dsRef;

    private static boolean VERBOSE = false;

    @Test public void testAcquireLock() {
        if ( VERBOSE ) {
            System.out.println("testAcquireLock");
            LogLock.verbose();
            LogLockMgr.verbose();
        }

        DeltaLink dLink = getDLink();

        LogLockMgr mgr = new LogLockMgr(dLink);
        // Drive the LogLockMgr manually.
        // Do not start the background thread.

        LogLock lock = new LogLock(dLink, dsRef);
        LogLock lock2 = new LogLock(dLink, dsRef);

        if ( VERBOSE )
            System.out.println("Acquire 1");
        lock.acquireLock();
        mgr.add(lock); // Check on begin.

        async(()->{
            if ( VERBOSE )
                System.out.println("Pause and refresh");
            // Refresh a few times and stop
            for ( int i = 0 ; i < 5 ; i++ ) {
                Lib.sleep(750);
                mgr.refresh();
            }
            if ( VERBOSE )
                System.out.println("Stop refresh");
        });

        // Start a delayed operation to release the lock.
        async(()->{
            Lib.sleep(1000);
            if ( VERBOSE )
                System.out.println("Release");
            lock.releaseLock();
            if ( VERBOSE )
                System.out.println("Remove");
            mgr.remove(lock);
            });


        // Acquire by the other java object.
        if ( VERBOSE )
            System.out.println("Acquire 2");
        boolean b = lock2.acquireLock();
        lock2.releaseLock();
        if ( VERBOSE )
            System.out.println("End - testAcquireLock");
    }

    @Test public void testContendTxn() {
        if ( VERBOSE ) {
            System.out.println("testContendTxn");
            LogLock.verbose();
            //LogLock.testMode();
            LogLockMgr.verbose();
        }

        Zone zone1 = Zone.connectMem();
        DeltaClient dClient1 = DeltaClient.create(zone1, getDLink());
        Zone zone2 = Zone.connectMem();
        DeltaClient dClient2 = DeltaClient.create(zone2, getDLink());

        // Two mirrors - same log
        DeltaConnection dConn1 = dClient1.register(dsRef, LocalStorageType.MEM,SyncPolicy.TXN_RW);
        DeltaConnection dConn2 = dClient2.register(dsRef, LocalStorageType.MEM,SyncPolicy.TXN_RW);

        LogLockMgr mgr = new LogLockMgr(getDLink());
        mgr.start();

        Semaphore sema1 = new Semaphore(0);
        Semaphore sema2 = new Semaphore(0);

        Runnable r = ()-> {
            DatasetGraph dsg1 = dConn1.getDatasetGraph();

            Txn.executeWrite(dsg1, ()->{
                if ( VERBOSE )
                    System.out.println("1: In Txn");
                sema1.release();
                // Long wait.
                Lib.sleep(2000);
                if ( VERBOSE )
                    System.out.println("1: Leave Txn");
            });
            if ( VERBOSE )
                System.out.println("1: Left Txn");
            sema2.release();
        };

        try {
            async(r);
            sema1.acquire();
//            LockState state = dLink.readLock(dConn1.getDataSourceId());
//            System.out.println(state);
            if ( VERBOSE )
                System.out.println("2: Enter Txn");
            DatasetGraph dsg2 = dConn2.getDatasetGraph();


            Txn.executeWrite(dsg2, ()->{
                if ( VERBOSE )
                    System.out.println("2: In Txn");
            });
            if ( VERBOSE )
                System.out.println("2: Left Txn");
            sema2.acquire();
            if ( VERBOSE )
                System.out.println("End - testContendTxn");
        }
        catch (Exception ex) { ex.printStackTrace(); }
    }
}
