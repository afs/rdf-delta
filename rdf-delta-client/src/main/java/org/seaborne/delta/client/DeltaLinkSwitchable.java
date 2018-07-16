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

import static java.lang.String.format;

import java.util.List;
import java.util.function.Supplier;

import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.DeltaLinkWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaLinkSwitchable extends DeltaLinkWrapper {
    private static Logger LOG = LoggerFactory.getLogger(DeltaLinkSwitchable.class);

    /** Suppress switchover warnings (for tests, where switchovers are expected) */ 
    public static boolean silentSwitchOver = true;
    
    /* DeltalLink operations are "retryable" except for append. If one fails, then it can
     * be re-executed for the same effect.
     * 
     * For {@link #append} retry may get an error.
     * 
     * Delta patch servers do (by contract) allow a repeat of the current head item and
     * give a non-error return (200 for a HTTP link). 
     * A failure indicating the patch is not the head (some other patch sneaked in, which can happen anyway).
     */

    private List<DeltaLink> others;
    private volatile int current = 0;
    private volatile DeltaLink currentLink;
    
    public DeltaLinkSwitchable(List<DeltaLink> others) {
        super(null);
        if ( others.isEmpty() )
            throw new IllegalArgumentException("Empty list of DeltaLinks to switch between");
        this.others = others;
        current = 0;
        currentLink = others.get(current);
    }
    
    @Override
    protected DeltaLink get() { return currentLink; }

    // Execution policies.
    // Note: DeltalLink operations are "retryable" with care.
    @Override
    protected <T> T execRtn(Supplier<T> action) {
        check();
        try { 
            return action.get();
        } catch (RuntimeException ex) {
            exceptionSwitching(ex);
            switchoverCurrentLink();
            return action.get();
        }
    }

    @Override
    protected void exec(Runnable action) {
        check();
        try { 
            action.run();
        } catch (RuntimeException ex) {
            exceptionSwitching(ex);
            switchoverCurrentLink();
            action.run();
        }
    } 

    private static void exceptionSwitching(RuntimeException ex) {
        //System.err.printf("Switching: %s\n", ex.getMessage());
        //ex.printStackTrace();
        if ( ! silentSwitchOver )
            FmtLog.warn(LOG, "HTTP failure switch over: %s", ex.getMessage());
    }

    /** Ask to switch links */ 
    public void switchover() {
        LOG.info("Application-requested witchover");
        switchoverCurrentLink();
    }
        
    private void switchoverCurrentLink() {
        DeltaLink lastLink = currentLink;
        int last = current;
        if ( others.size() == 1 )
            throw new DeltaException("One one link : Can't find a replacement DeltaLink on switchover");
        
        // precaution: limit re-links.
        for(int i = 0 ; i < others.size(); i++ ) { 
            try { 
                current = (current+1) % others.size();
                currentLink = others.get(current);
                if ( last == current )
                    // currentLink "reset"
                    throw new DeltaException("Can't find a replacement DeltaLink on switchover");
                FmtLog.info(LOG, "Switch %s to %s", lastLink, currentLink);
                currentLink.ping();
                return;
            } catch (RuntimeException ex) { throw ex; } 
        }
    }
    
    private void check() {
        if ( !others.isEmpty() ) {
            if ( others.get(current) != currentLink )
                throw new IllegalStateException("Current not set correctly");
        }
    }

    @Override
    public String toString() {
        return format("DeltaLinkSwitchable: current=%d %s", current, others);
    }
}
