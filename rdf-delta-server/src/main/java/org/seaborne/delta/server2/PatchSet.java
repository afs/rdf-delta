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

package org.seaborne.delta.server2;

import java.util.* ;
import java.util.concurrent.ConcurrentHashMap ;

import org.apache.jena.atlas.logging.FmtLog ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

/** Collection of patches for on dataset */ 
public class PatchSet {
    // Centralized logger fro regualr lifecyle reporting.
    private static Logger LOG = LoggerFactory.getLogger(PatchSet.class) ;
    // Tree?
    
    // All patches.
    private Map<UUID, Patch> patches = new ConcurrentHashMap<>() ;
    
    // The mainline. This enables going forward.
    private LinkedList<Patch> history = new LinkedList<>() ;
    private final UUID id ; 
    
    private List<PatchHandlerFactory> enqueuedNotifications = new ArrayList<>() ;  
    
    public PatchSet(UUID id) {
        this.id = id ;
    }

    public PatchSetInfo getInfo() {
        return new PatchSetInfo(0L, 0L, id, null) ;
    }
    
    public void add(Patch patch) {
        // Validate.
        patches.put(patch.getId(), patch) ;
        
        if ( Objects.equals(currentHead(), patch.getParent()) ) {
            history.add(patch) ;
            FmtLog.warn(LOG, "Patch queued: id=%s", patch.getId()) ;
            enqueuedNotifications.forEach( phf -> phf.handle().handle(patch));
        } else 
            FmtLog.warn(LOG, "Patch is out of sequence - not queued as a version: id=%s", patch.getId()) ; 
    }
    
    private UUID currentHead() {
        if ( history.isEmpty() )
            return null ;
        return history.getLast().getParent() ;
    }

    public void process(UUID id, PatchHandler c) {
        Patch patch = patches.get(id) ;
    }
    
    public void processFrom(UUID start, PatchHandler c) {
        int idx = findStart(start) ; 
        if ( idx < 0 ) {
            FmtLog.warn(LOG, "Didn't find a patch: %s", start) ;
            return ;
        }
        
        Iterator<Patch> iter = history.listIterator(idx) ; 
        iter.forEachRemaining((p)-> c.handle(p) ) ;
    }

    private int findStart(UUID start) {
        return history.indexOf(start) ;
    }
    
    // Clear out.
    
}
