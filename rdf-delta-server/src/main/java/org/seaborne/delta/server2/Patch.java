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

import java.io.InputStream ;
import java.lang.ref.WeakReference ;
import java.util.UUID ;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.logging.Log ;
import org.seaborne.patch.PatchReader ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.changes.RDFChangesCollector ;

/** A single patch, backed by a disk copy */ 
public class Patch {

    private WeakReference<RDFPatch> patchRef ;
    // The persistent record of this patch used to regenerate  
    private final String filename ;
    private final UUID id ;
    private final UUID parent ; 
    
    public Patch(UUID name, UUID parent, RDFPatch patch, String filename) {
        this.patchRef = new WeakReference<>(patch) ;
        this.filename = filename ;
        this.id = name ;
        this.parent = parent ; 
    }
    
    public UUID getId() {
        return id ;
    }

    public UUID getParent() {
        return parent ;
    }

    /** Play this patch through the {@link RDFChanges} provided. */ 
    public void play(RDFChanges dest) {
        RDFPatch p = patchRef.get() ;
        if ( p == null ) {
            if ( filename == null ) {
                Log.warn(this, "No patch, no disk copy") ;
                return ;
            }
            p = fileToPatch(filename) ;
            patchRef = new WeakReference<>(p) ;
        }
        p.apply(dest);
    }
    
    // XXX Put somewhere library-ish
    private static RDFPatch fileToPatch(String filename) {
        InputStream in = IO.openFile(filename) ;
        PatchReader pr = new PatchReader(in) ;
        RDFChangesCollector c = new RDFChangesCollector() ;
        pr.apply(c);
        return c.getRDFPatch() ; 
        
    }
}
