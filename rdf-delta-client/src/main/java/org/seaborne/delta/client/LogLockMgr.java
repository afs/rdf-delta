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

import static java.util.stream.Collectors.toList;
import static org.seaborne.delta.DeltaConst.F_ARRAY;
import static org.seaborne.delta.DeltaConst.F_DATASOURCE;
import static org.seaborne.delta.DeltaConst.F_LOCK_REF;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.LogCtl;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client-side manager of locks for one {@link DeltaLink}, that is all locks on one server. */
public class LogLockMgr {
    private static Logger LOG = LoggerFactory.getLogger(LogLockMgr.class);

    // Manage logs per server. LogLockMgr.
    private Set<LogLock> active = ConcurrentHashMap.newKeySet();
    //private Set<Id> activeLocks = ConcurrentHashMap.newKeySet();
    //private Runnable lockRefresher1 = ()-> active.forEach(lock->lock.refreshLock());

    // Development only!
    private static boolean DEBUG = false;

    // => Step 3: Grab after

    public static void testMode() {
        LogCtl.enable(LOG);
        DEBUG = true;
    }

    private static void DEV(String fmt, Object... args) {
        if ( DEBUG )
            FmtLog.debug(LOG,  fmt,  args);
    }

    // Ideal: batch refresh operation { array: [ { datasource: "", lock: ""} ] }
    private Runnable lockRefresher = ()-> {
        DEV("REFRESH %d", active.size());
        active.forEach(lock->lock.refreshLock());
    };

    private static int INITIAL_DELAY_MS = 1000;
    private static int REFRESH_MS = 1000;

    private ScheduledExecutorService executor = null;
    private final DeltaLink dLink;
    public DeltaLink getLink() { return dLink; }

    public LogLockMgr(DeltaLink dLink) {
        this.dLink = dLink;
    }

    public void refresh() {
        lockRefresher.run();
    }

    public void startManual() {
        // Without a background thread.
    }

    public void start() {
        executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(lockRefresher, INITIAL_DELAY_MS, REFRESH_MS, TimeUnit.MILLISECONDS);
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

    /*package*/ static JsonObject buildRefreshArg(Set<LogLock> locks) {
        List<JsonObject> x = locks.stream().map(lock->{
            return JSONX.buildObject(b->{
                b.pair(F_DATASOURCE, lock.getDataSourceId().asPlainString());
                b.pair(F_LOCK_REF, lock.getLockSessionId().asPlainString());
            });
        }).collect(toList());
        JsonArray array = new JsonArray();
        array.addAll(x);
        return JSONX.buildObject(b->b.pair(F_ARRAY, array));
    }
}
