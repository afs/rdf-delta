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

package org.seaborne.delta.client;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.web.HttpSC;
import org.seaborne.delta.Delta;
import org.seaborne.delta.Id;
import org.seaborne.delta.LockState;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger;

/** A patch log lock */
public class LogLock {

    private static Logger LOG = Delta.DELTA_CLIENT;

    // Keep for debugging
    private DeltaConnection dConn;

    private final DeltaLink dLink;

    private final Id dataSourceId;
    private final AtomicReference<Id> lockSessionId = new AtomicReference<>();

    public LogLock(DeltaConnection dConn) {
        this(dConn.getLink(), dConn.getDataSourceId());
        this.dConn = dConn;
    }

    public LogLock(DeltaLink dLink, Id datasourceId) {
        this.dLink = dLink;
        this.dataSourceId = datasourceId;
    }

    public DeltaConnection getConnection() {
        return dConn;
    }

    public DeltaLink getLink() {
        return dLink;
    }

    public Id getDataSourceId() {
        return dataSourceId;
    }

    public Id getLockSessionId() {
        return lockSessionId.get();
    }

    public boolean acquireLock() {
        Id session = _acquireLock(dLink, dataSourceId);
        if ( session == null )
            return false;
        lockSessionId.set(session);
        return true;
    }

    public boolean refreshLock() {
        Id session = lockSessionId.get();
        if ( session == null )
            return false;
        return _refreshLock(dLink, dataSourceId, session);
    }

    public LockState readLock() {
        return _readLock(dLink, dataSourceId);
    }

    public Id grabLock() {
        Id session = lockSessionId.get();
        if ( session == null )
            return null;
        return _grabLock(dLink, dataSourceId, session);
    }


    // XXX read to check.
    public boolean isLocked() { return lockSessionId.get() != null ; }
    //public Id lookupLock() { return null; }

    public void releaseLock() {
        Id sessionId =  lockSessionId.get();
        if ( sessionId == null )
            return;
        _releaseLock(dLink, dataSourceId, sessionId);
        lockSessionId.set(null);
    }

    // => Step 3: Grab after



        // => Step 3: Grab after



        // ---- TEMP
    //    // ---- Simple algorithm
    //    /** Number of attempts to acquire-read the lock within a full cycle. */
    //    private static int LOCK_V0_ACQUIRE_READ_RETRIES     = 10 ;
    //
    //    /** Gap between attempts to acquire/read the patch log lock (milliseconds). */
    //    private static int LOCK_V0_ACQUIRE_READ_WAIT_MS     = 500;
    //    // ---- Simple algorithm
    //
    //    /**
    //     * Simple.
    //     * Poll to get the lock.
    //     * A crashed lock holder blocks the system.
    //     */
    //    private static Id _acquireLock0(DeltaLink dLink, Id datasourceId) {
    //        try {
    //            int attempts = 0;
    //            for(;;) {
    //                Id lockSession = dLink.acquireLock(datasourceId);
    //                if ( lockSession != null )
    //                    return lockSession;
    //                attempts++;
    //                if ( attempts >= LOCK_V0_ACQUIRE_READ_RETRIES ) {
    //                    DEV("Failed to get lock after %d attempts+", attempts);
    //                    return null;
    //                }
    //                Lib.sleep(LOCK_V0_ACQUIRE_READ_WAIT_MS);
    //            }
    //        } catch (HttpException ex) {
    //            FmtLog.warn(LOG, "Failed to accquire the patch log lock: %s", datasourceId);
    //            if ( ex.getStatusCode() == -1 )
    //                throw new HttpException(HttpSC.SERVICE_UNAVAILABLE_503, HttpSC.getMessage(HttpSC.SERVICE_UNAVAILABLE_503), ex.getMessage());
    //            throw ex;
    //        }
    //        // Start ticking.
    //    }

        private static Id _acquireLock(DeltaLink dLink, Id datasourceId) {
            try {
                Id id = attemptToAcquireLock(0, dLink, datasourceId);
                DEV("==> acquireLock (%s, %s)", datasourceId, id);
                return id;
            } catch (HttpException ex) {
                failedConnection();
                FmtLog.warn(LOG, "Failed to acquire the patch log lock: %s", datasourceId);
                if ( ex.getStatusCode() == -1 )
                    throw new HttpException(HttpSC.SERVICE_UNAVAILABLE_503, HttpSC.getMessage(HttpSC.SERVICE_UNAVAILABLE_503), ex.getMessage());
                throw ex;
            }
        }

    private static boolean _refreshLock(DeltaLink dLink, Id datasourceId, Id lockSession) {
        try {
            return dLink.refreshLock(datasourceId, lockSession);
        } catch (HttpException ex) {
            failedConnection();
            FmtLog.warn(LOG, "Failed to refresh the patch log lock: %s", datasourceId);
            if ( ex.getStatusCode() == -1 )
                throw new HttpException(HttpSC.SERVICE_UNAVAILABLE_503, HttpSC.getMessage(HttpSC.SERVICE_UNAVAILABLE_503), ex.getMessage());
            throw ex;
        }
    }

    private static LockState _readLock(DeltaLink dLink, Id datasourceId) {
        try {
            return dLink.readLock(datasourceId);
        } catch (HttpException ex) {
            failedConnection();
            FmtLog.warn(LOG, "Failed to read the patch log lock: %s", datasourceId);
            if ( ex.getStatusCode() == -1 )
                throw new HttpException(HttpSC.SERVICE_UNAVAILABLE_503, HttpSC.getMessage(HttpSC.SERVICE_UNAVAILABLE_503), ex.getMessage());
            throw ex;
        }
    }

    private static Id _grabLock(DeltaLink dLink, Id datasourceId, Id oldLockSession) {
        try {
            return dLink.grabLock(datasourceId, oldLockSession);
        } catch (HttpException ex) {
            FmtLog.warn(LOG, "Failed to grab the patch log lock: %s", datasourceId);
            if ( ex.getStatusCode() == -1 )
                throw new HttpException(HttpSC.SERVICE_UNAVAILABLE_503, HttpSC.getMessage(HttpSC.SERVICE_UNAVAILABLE_503), ex.getMessage());
            failedConnection();
            throw ex;
        }
    }

    /**
     * Release the patch log lock.
     * If there is an error, the patch service is probably down (single server version).
     * All we can do is ignore, and resync later.
     */
    private static void _releaseLock(DeltaLink dLink, Id datasourceId, Id lockSession) {
        try {
            dLink.releaseLock(datasourceId, lockSession);
        } catch (HttpException ex) {
            FmtLog.warn(LOG, "Release lock failed: %s", datasourceId.toString());
        }
    }

    private static void failedConnection() {}

    // ---- The acquire algorithm.

    // Development only!
    private static boolean DEBUG = false;

    // => Step 3: Grab after

    public static void testMode() {
        LogCtl.enable(LOG);
        LOCK_ACQUIRE_MAX_DEPTH = 2 ;
        LOCK_POLL_WAIT_MS = 500;
        LOCK_SAME_TICKS_RETRIES = 3;
        LOCK_STATE_CHANGE_RETRIES = 2;
        DEBUG = true;
    }

    // => Step 3: Grab after

    private static void DEV(String fmt, Object... args) {
        if ( DEBUG )
            FmtLog.debug(LOG,  fmt,  args);
    }

    /** Maximum number of full cycles of acquire / poll read / someone else grabs the lock or lock becomes free. */
    private static int LOCK_ACQUIRE_MAX_DEPTH           = 5 ;

    /** Number of attempts watching for the session to change or ticks not to change. */
    private static int LOCK_SAME_TICKS_RETRIES          = 60;

    // => Step 2: max 30s to see lock advance or change.

    /** Number of times to read the lock when the session changes. */
    private static int LOCK_STATE_CHANGE_RETRIES        = 10;

    /** Gap between attempts to read the patch log lock (milliseconds). */
    private static int LOCK_POLL_WAIT_MS                = 500;

    /**
     * Acquire the patch log lock else bail out.
     * <p>
     * The patch log is a discretion lock provided by the patch log server that can be
     * used to serialise updates across the cluster.
     * <p>
     * The server provides certain actions on locks, none of which block or wait. The
     * policy for the lock is determined by the clients.
     * <p>
     * This is not perfect - it does not always acquire the lock when unexpected
     * combinations of failures occur. It is intended first and foremost to protect the
     * data, in some cases by rejecting an "acquire" request. The algorithm is skewed to
     * address the typical use cases of a predominately read workload, or updates being
     * directed via one distinguished replica.
     * <p>
     * The server-side lock has token to identify the current critical section; it also
     * has a ticks field that is incremented to show the current lock owner is still alive
     * and making progress.
     * <p>
     * If an update request sees the lock increasing, it waits for the lock to become
     * free. If it sees the ticks field stop changing, then after a timeout, it can grab
     * the lock, breaking the presumed dead session.
     * <p>
     * The patch log protocol checks that a patch names the previous patch in the log so
     * if a update request on one machine grabs the lock for itself, and the original
     * request is still going, one or other will be bounced when the append the patch
     * because they can't both name the same head of the log.
     * <p>
     * This optimistic concurrency failure is what protects the integrity of the log.
     * <p>
     * Algorithm:
     * <ul>
     * <li> Try to acquire the lock and return if successful
     * <li> Otherwise, read the lock state (being careful the lock may become free at this point).
     * <li> Poll, noting whether the lock ticks are advancing, or the lock becomes free, or for someone else
     * to get it. Id the ticks advance, loop.
     * <li> If the lock became free, start again.
     * <li> If someone else grabbed it, start again.
     * <li> If the ticks aren't advancing, grab (break and acquire) the lock.
     * <li>If waiting too long (there is a slow writer), grab (break and acquire).
     *     This is not an ideal outcome and has a quite long timeout.
     * </ul>
     *
     * This is not perfect. It is intended first and foremost to protect the data, in some
     * cases by rejecting an "acquire" request. The algortihm is skewed to address the
     * typical use cases of a predominately read workload, or updates being directed via
     * one distinguished replica.
     * <ul>
     * <li>Waiting for the lock is not a queue. If the system is under heavy write load,
     * one writer may not manage to acquire the lock because it does not manage to get the
     * lock when all the waiting writers try to acquire the lock.
     * <li>
     * </ul>
     */
    private static Id attemptToAcquireLock(int depth, DeltaLink dLink, Id datasourceId) {
        // One attempt. If someone else grabs the lock, while we are waiting, go down one depth (tail recursion). */
        DEV("acquireLockOneCycle(%d, %s)", depth, datasourceId);
        if ( depth > LOCK_ACQUIRE_MAX_DEPTH ) {
            // Protect against excessive retries.
            FmtLog.warn(LOG, "Failed to initially acquire lock after %d cycles", depth);
            return null;
        }
        // This is stackdepth = restart attempts.
        depth++;

        // Step 1: Acquire the lock or read the lock.
        DEV(">%d Attempt to acquire lock: %s", depth, datasourceId);
        Id lockSession = dLink.acquireLock(datasourceId);

        if ( lockSession != null )
            // Success!
            return lockSession;

        // Step 2: Read the state.
        //DEV(">%d Read lock: %s", depth, datasourceId);
        LockState state = dLink.readLock(datasourceId);
        if ( state == LockState.FREE )
            // Lock released. Restart.
            return attemptToAcquireLock(depth, dLink, datasourceId);

        DEV(">%d Initial read lock: %s state=%s", depth, datasourceId, state);

        // Step 3: Poll the lock.
        // Someone else has the lock.
        // Start polling by reading the lock and checking the ticks make progress indicating the holder is still alive.

        // Our record of the state.
        int pollWaitAttempts = 0;
        Id sessionToken = state.session;

        // One loop for each change of lock ownership (someone else grabs).
        // pollReadLock includes the tracking of ticks advancing.
        for(;;) {
            pollWaitAttempts++;
            if ( pollWaitAttempts > LOCK_STATE_CHANGE_RETRIES )
                break;
            // Loop looking for ticks change.
            DEV(">%d Poll lock : %s", depth, datasourceId);

            LockState state2 = pollReadLock(dLink, datasourceId, state);

            if ( state2 == LockState.FREE ) {
                DEV("Lock became free: %s", datasourceId);
                // Lock now free : restart.
                return attemptToAcquireLock(depth, dLink, datasourceId);
            }

            DEV(">%d Observe lock: (poll=%d) %s state=%s", depth, pollWaitAttempts, datasourceId, state);
            // Two cases: same session, ticks have not advanced; and different session (any ticks)
            Id sessionToken2 = state2.session;
            if ( sessionToken.equals(sessionToken2) )
                // Ticks not advancing.
                break;

            // Someone else grabbed the session
            DEV("Restart/other party");
            // Move to new state.
            state = state2;
            // Loop
        }

        FmtLog.warn(LOG, "Grabbing the lock: "+datasourceId);

        // Lock has been changing but we have waited too long.
        // Grab the lock.

        Id id = dLink.grabLock(datasourceId, sessionToken);
        DEV("Grab: "+id);

        if ( id != null )
            // Grab succeeds.
            return id;
        // Grab fails. Someone else got it. Start again.
        return attemptToAcquireLock(depth, dLink, datasourceId);
    }

    /** Watch the lock advance.
     * Loop watching for the lock
     * <ul>
     * <li> If the the ticks do not advance, timeout and return null.
     * <li>
     * <li>ticks advance, reset the observed tick setting and keep polling
     * <li>if the lock changes owner, return the new lock state
     * </ul>
     */
    private static LockState pollReadLock(DeltaLink dLink, Id datasourceId, LockState lockState) {
        int totalAttempts = 0;
        int sameTickAttempts = 0;

        for(;;) {
            Id lockSession = lockState.session;
            long ticks = lockState.ticks;
            sameTickAttempts++;
            totalAttempts++;
            if ( sameTickAttempts > LOCK_SAME_TICKS_RETRIES ) {
                DEV("{%d} Lock not advancing - end polling", totalAttempts);
                return lockState;
            }
            Lib.sleep(LOCK_POLL_WAIT_MS);
            DEV("{%d} ticks=%s", totalAttempts, ticks);
            LockState state2 = dLink.readLock(datasourceId);
            if ( state2 == LockState.FREE ) {
                // Lock became free.
                DEV("Poll lock attempt=%d - lock became free", sameTickAttempts);
                return LockState.FREE;
            }

            DEV("{%d} lock state %s", totalAttempts, state2);
            Id sessionToken2 = state2.session;
            long ticks2 = state2.ticks;

            if ( ! Objects.equals(lockSession, sessionToken2) ) {
                // Lock session id changed.
                DEV("{%d} owner changed to %s", totalAttempts, sessionToken2);
                return state2;
            }
            if ( ticks2 > ticks ) {
                DEV("{%d} ticks advanced %d", totalAttempts, ticks2);
                lockState = state2;
                sameTickAttempts = 0;
            } else
                DEV("{%d} ticks did not advance", totalAttempts);
            // And loop.
        }
    }
}
