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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.web.HttpSC;
import org.seaborne.delta.Delta;
import org.seaborne.delta.Id;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger;

public class LogLock {

    private static Logger LOG = Delta.DELTA_CLIENT;

    // Keep for debugging
    private DeltaConnection dConn;

    private final DeltaLink dLink;
    private final Id dataSourceId;
    private final AtomicReference<Id> lockSessionId = new AtomicReference<>();

    static Set<LogLock> active = ConcurrentHashMap.newKeySet();
    static Runnable lockRefresher = ()-> active.forEach(lock->lock.refreshLock());

    static ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    static {
        executor.schedule(lockRefresher, 10, TimeUnit.SECONDS);
    }

    public LogLock(DeltaConnection dConn) {
        this(dConn.getLink(), dConn.getDataSourceId());
        this.dConn = dConn;
    }

    public LogLock(DeltaLink dLink, Id datasourceId) {
        this.dLink = dLink;
        this.dataSourceId = datasourceId;
    }

    public void acquireLock() {}
    public void refreshLock() {}
    public void releaseLock() {}



    /**
     * Acquire the patch log lock else bail out.
     */
    private Id _acquireLock() {
        try {
            Id lockId = dLink.acquireLock(dConn.getDataSourceId());
            lockSessionId.set(lockId);
            return lockId;
        } catch (HttpException ex) {
            FmtLog.warn(LOG, "Failed to accquire the patch log lock: %s", dConn.getDataSourceId());
            if ( ex.getStatusCode() == -1 )
                throw new HttpException(HttpSC.SERVICE_UNAVAILABLE_503, HttpSC.getMessage(HttpSC.SERVICE_UNAVAILABLE_503), ex.getMessage());
            throw ex;
        }
    }

    // Ticker.

    private void localCancel(Id lockOwnership) {

    }


    /**
     * Release the patch log lock.
     * If there is an error, the patch service is probably down (single server version).
     * All we can do is ignore, and resync later.
     */
    private void releaseLock(Id lockOwnership) {
        if ( lockOwnership == null )
            return;
        if ( ! lockOwnership.equals(lockSessionId.get()) ) {
            lockSessionId.set(null);
            return;
        }
        try {
            dLink.releaseLock(dConn.getDataSourceId(), lockOwnership);
        } catch (HttpException ex) {
            FmtLog.warn(LOG, "Release lock failed: %s", dConn.getDataSourceId());
        }
    }

    /*package*/ void refreshLock(Id lockOwnership) {
        if ( lockOwnership == null )
            return;
        try {
            //dLink.refreshLock(dConn.getDataSourceId(), lockOwnership);
        } catch (HttpException ex) {
            FmtLog.warn(LOG, "Refresh lock failed: %s", dConn.getDataSourceId());
        }
    }
}
