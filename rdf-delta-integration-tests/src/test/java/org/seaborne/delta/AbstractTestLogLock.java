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
import static org.junit.Assert.*;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.seaborne.delta.link.DeltaLink;

/** Tests of LogLock functionality. */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractTestLogLock {

    private static Id dsRef;
    private static AtomicInteger counter = new AtomicInteger(0);

    @Before public void beforeTest() {
        if ( dsRef != null )
            return;
        int x = counter.incrementAndGet();
        String name = String.format("ABC%02d",x);
        dsRef = getDLink().newDataSource(name, "http://example/"+name);
    }

    @After public void afterTest() {
        getDLink().removeDataSource(dsRef);
        dsRef = null;
    }

    protected abstract DeltaLink getDLink();

    @Test public void deltaLinkLock_acquire_release_01() {
        // Acquire-release
        Id lockRef = getDLink().acquireLock(dsRef);
        assertNotNull(lockRef);
        getDLink().releaseLock(dsRef, lockRef);
        LockState lockState = getDLink().readLock(dsRef);
        assertTrue(LockState.isFree(lockState));
        assertEquals(LockState.UNLOCKED, lockState);
    }

    @Test public void deltaLinkLock_acquire_release_02() {
        // Acquire-release-release
        Id lockRef = getDLink().acquireLock(dsRef);
        getDLink().releaseLock(dsRef, lockRef);
        //safe
        getDLink().releaseLock(dsRef, lockRef);
    }

    @Test public void deltaLinkLock_refresh_01() {
        // Acquire-refresh-release
        Id lockRef = getDLink().acquireLock(dsRef);
        boolean b1 = getDLink().refreshLock(dsRef, lockRef);
        assertTrue("Lock refresh while owning the lock", b1);
        getDLink().releaseLock(dsRef, lockRef);
    }

    @Test public void deltaLinkLock_refresh_02() {
        // No acquire-refresh-release
        LockState lockState = getDLink().readLock(dsRef);
        assertTrue(LockState.isFree(lockState));

        boolean b1 = getDLink().refreshLock(dsRef, Id.create());
        assertFalse("Lock refresh without owning the lock", b1);
        Id lockRef = getDLink().acquireLock(dsRef);
        getDLink().releaseLock(dsRef, lockRef);
        boolean b2 = getDLink().refreshLock(dsRef, Id.create());
        assertFalse("Lock refresh after releasing the lock", b2);
    }

    @Test public void deltaLinkLock_read_01() {
        LockState state = getDLink().readLock(dsRef);
        assertEquals(LockState.UNLOCKED, state);
    }

    @Test public void deltaLinkLock_read_02() {
        Id lockRef = getDLink().acquireLock(dsRef);
        LockState state = getDLink().readLock(dsRef);
        Id lockSessionId = state.session;
        long ticker = state.ticks;
        assertEquals(lockRef, lockSessionId);
        assertEquals(1L, ticker);
        getDLink().releaseLock(dsRef, lockSessionId);
    }

    @Test public void deltaLinkLock_read_03() {
        Id lockRef = getDLink().acquireLock(dsRef);
        getDLink().releaseLock(dsRef, lockRef);
        LockState state = getDLink().readLock(dsRef);
        assertEquals(LockState.UNLOCKED, state);
    }

    @Test public void deltaLinkLock_refresh_read_01() {
        Id lockRef = getDLink().acquireLock(dsRef);
        long ticker0 = getDLink().readLock(dsRef).ticks;

        getDLink().refreshLock(dsRef, lockRef);
        LockState state = getDLink().readLock(dsRef);
        long ticker1 = state.ticks;

        assertTrue(ticker1 > ticker0);
        assertEquals(2L, ticker1);
    }

    // Lock breaking.
    @Test public void deltaLinkLock_grab_01() {
        Id lockRef1 = getDLink().acquireLock(dsRef);
        Id lockRef2 = getDLink().grabLock(dsRef, lockRef1);

        assertNotEquals("After grab", lockRef1, lockRef2);
        getDLink().releaseLock(dsRef, lockRef2);

        LockState state = getDLink().readLock(dsRef);
        assertEquals("Read after release", LockState.UNLOCKED, state);
    }

    @Test public void deltaLinkLock_grab_02() {
        Id lockRef1 = getDLink().acquireLock(dsRef);
        Id lockRef2 = getDLink().grabLock(dsRef, lockRef1);

        assertNotEquals(lockRef1, lockRef2);
        //getDLink().releaseLock(dsRef, lockRef1);

        LockState state1 = getDLink().readLock(dsRef);
        assertNotEquals("Read after grab, before release", state1, LockState.UNLOCKED);
        assertEquals(lockRef2, state1.session);
        assertEquals(1L, state1.ticks);

        getDLink().releaseLock(dsRef, lockRef2);
        LockState state2 = getDLink().readLock(dsRef);
        assertEquals("Read after grab-release", LockState.UNLOCKED, state2);
    }

    @Test public void deltaLinkLock_grab_03() {
        Id lockRef1 = getDLink().acquireLock(dsRef);
        Id lockRef2 = getDLink().grabLock(dsRef, lockRef1);

        // Fails.
        Id lockRef3 = getDLink().grabLock(dsRef, Id.create());
        assertNull("Second grab", lockRef3);
    }

    @Test public void deltaLinkLock_grab_04() {
        Id lockRef = getDLink().grabLock(dsRef, Id.create());
        assertNull("Unlocked grab", lockRef);
    }

    // Lock contention.
    @Test(timeout=500) public void deltaLinkLock_contend_01() {
        Id lockRef1 = getDLink().acquireLock(dsRef);
        assertNotNull("Acquire 1", lockRef1);
        // Non-blocking.
        Id lockRef2 = getDLink().acquireLock(dsRef);
        assertNull("Acquire 2", lockRef2);
    }

    @Test(timeout=500) public void deltaLinkLock_contend_02() {
        Id lockRef1 = getDLink().acquireLock(dsRef);
        Id lockRef2 = getDLink().acquireLock(dsRef);

        getDLink().releaseLock(dsRef, lockRef1);

        // Now get the lock.
        Id lockRef3 = getDLink().acquireLock(dsRef);
        assertNotNull(lockRef3);
        getDLink().releaseLock(dsRef, lockRef3);
    }

    @Test(timeout=500)
    public void deltaLinkLock_sequence_01() throws InterruptedException {
        Semaphore sema1 = new Semaphore(0);
        Semaphore sema2 = new Semaphore(0);
        AtomicReference<Throwable> asyncOutcome = new AtomicReference<>();

        async(()->{
            try {
                Id lockRef1 = getDLink().acquireLock(dsRef);
                // Now inside the lock.
                sema1.release();
                // Wait for main thread to let us proceed.
                sema2.acquire();
                getDLink().releaseLock(dsRef, lockRef1);
                // We're done.
                sema1.release();
            } catch (Exception ex) {
                asyncOutcome.set(ex);
            }
        });
        // Wait until async is inside the lock.
        sema1.acquire();
        // Other thread now waiting on sema2 inside the lock.
        Id lockRef2 = getDLink().acquireLock(dsRef);
        // We don't get the lock.
        assertNull(lockRef2);

        // Have async move forward and release the lock.
        sema2.release();
        // Wait for async done.
        sema1.acquire();

        // Now we can get the lock.
        Id lockRef3 = getDLink().acquireLock(dsRef);
        assertNotNull(lockRef3);
    }
}
