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

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jena.atlas.json.*;
import org.apache.jena.atlas.logging.Log;
import static org.seaborne.delta.DeltaConst.*;

import org.seaborne.delta.Id;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.link.DeltaLink;

/** Client-side manager of locks for one {@link DeltaLink}, that is all locks on one server. */
public class LogLockMgr {
    // Manage logs per server. LogLockMgr.
    private Set<LogLock> active = ConcurrentHashMap.newKeySet();
    private Set<Id> activeLocks = ConcurrentHashMap.newKeySet();
    private Runnable lockRefresher1 = ()-> active.forEach(lock->lock.refreshLock());

    // Ideal: batch refresh operation { array: [ { datasource: "", lock: ""} ] }
    private Runnable lockRefresher = ()-> {
        //Set<Id> y = getLink().refreshLocks(activeLocks);

    };

    private ScheduledExecutorService executor = null;
    private final DeltaLink dLink;
    public DeltaLink getLink() { return dLink; }

    public LogLockMgr(DeltaLink dLink) {
        this.dLink = dLink;
        executor.schedule(lockRefresher, 5, TimeUnit.SECONDS);
    }

    public void start() {
        executor = Executors.newScheduledThreadPool(1);
        executor.schedule(lockRefresher, 5, TimeUnit.SECONDS);
    }

    public void stop() {
        try {
            boolean cleanShutdown = executor.awaitTermination(30, TimeUnit.SECONDS);
            if ( ! cleanShutdown )
                Log.warn(this,  "Did not cleanly shutdown: timeout awaitTermination");
        } catch (InterruptedException ex) {
            Log.warn(this,  "Did not cleanly shutdown: "+ex.getMessage());
        }
        executor = null;
    }


    /** Stop refreshing all locks - this operation does not cancel the lock. */
    public void dropAll() {

    }

    private void add(LogLock logLock) {
        active.add(logLock);
    }
    private void remove(LogLock logLock) {
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
