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

package org.seaborne.delta.client;

import static org.seaborne.delta.client.DeltaClientLib.threadFactoryDaemon;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.LogCtl;
import org.seaborne.delta.LockState;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client-side manager of locks for one {@link DeltaLink}, that is all locks on one server. */
public class LogLockMgr {

    private static Logger LOG = LoggerFactory.getLogger(LogLockMgr.class);

    // Manage logs per server. LogLockMgr.
    private Set<LogLock> active = ConcurrentHashMap.newKeySet();

    // Development only!
    private static boolean DEBUG = false;

    public static void verbose() {
        LogCtl.enable(LOG);
        DEBUG = true;
    }

    private static void DEV(String fmt, Object... args) {
        if ( DEBUG )
            FmtLog.debug(LOG,  fmt,  args);
    }

    // Ideal: batch refresh operation { array: [ { datasource: "", lock: ""} ] }
    private Runnable lockRefresher = ()-> {
        DEV("Refresh %d", active.size());
        active.forEach(lock-> {
            if ( lock.isLocked() ) {
                lock.refreshLock();
                if ( DEBUG ) {
                    LockState state = lock.readLock();
                    DEV("Refresh %s : %s", lock.getDataSourceId(), state);
                }
            }
        });
    };

    private static int LOCK_REFRESH_MS                  = 1000;
    private static int LOCK_REFRESH_INITIAL_DELAY_MS    = 500;

    private ScheduledExecutorService executor = null;
    private final DeltaLink dLink;
    public DeltaLink getLink() { return dLink; }

    public LogLockMgr(DeltaLink dLink) {
        this.dLink = dLink;
    }

    public void refresh() {
        lockRefresher.run();
    }

    public void start() {
        executor = Executors.newScheduledThreadPool(1, threadFactoryDaemon);
        executor.scheduleAtFixedRate(lockRefresher, LOCK_REFRESH_INITIAL_DELAY_MS, LOCK_REFRESH_MS, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if ( executor == null )
            return;
        executor.shutdownNow();
        executor = null;
        return ;
    }

//    /** Stop refreshing all locks - this operation does not cancel the lock. */
//    public void dropAll() {
//        active.clear();
//    }

    public void add(LogLock logLock) {
        DEV("add lock");
        active.add(logLock);
    }

    public void remove(LogLock logLock) {
        DEV("remove lock");
        active.remove(logLock);
    }
}
