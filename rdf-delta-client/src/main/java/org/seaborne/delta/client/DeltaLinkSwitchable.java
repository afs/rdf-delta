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

import java.util.List;
import java.util.function.Supplier;

import org.apache.jena.atlas.web.HttpException;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.DeltaLinkWrapper;

public class DeltaLinkSwitchable extends DeltaLinkWrapper {
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
    private int current = 0;
    private DeltaLink currentLink;
    
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
        try { 
            return action.get();
        } catch (HttpException ex) {
            switchover();
            return action.get();
        }
    }

    @Override
    protected void exec(Runnable action) {
        try { 
            action.run();
        } catch (HttpException ex) {
            switchover();
            action.run();
        }
    } 

    private void switchover() {
        int last = current;
        // precaution: limit re-links.
        for(int i = 0 ; i < others.size(); i++ ) { 
            try { 
                current = (current+1) % others.size();
                currentLink = others.get(current);
                if ( last == current )
                    // currentLink "reset"
                    throw new DeltaException("Can't find a replacement DeltaLink on switchover");
                currentLink.ping();
                return;
            } catch (Exception ex) { /* Ignore and loop */ } 
        }
    }
}
