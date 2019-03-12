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

package org.seaborne.delta.server.local.patchstores;

import java.util.List;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.ListUtils;
import org.seaborne.delta.Id;
import org.seaborne.patch.RDFPatch;

/**
 * Interface for the bulk storage of patches.
 */
public interface PatchStorage {
    /** Stream of all the patches - in no particular order */
    public Stream<Id> find();
    
    /** Store a patch */
    public void store(Id key, RDFPatch value);
    
    /** Get a patch */
    public RDFPatch fetch(Id key);
    
    /** Delete a patch */
    public void delete(Id id);
    
    /** Release all the patches and any other state for this {@code PatchStorage} */
    public default void release() { }
    
    /** Release all the patches and any other state for this {@code PatchStorage} */
    public default void delete() {
        // Copy to isolate.
        List<Id> x = ListUtils.toList(find()); 
        x.forEach(this::delete);
    }
}
