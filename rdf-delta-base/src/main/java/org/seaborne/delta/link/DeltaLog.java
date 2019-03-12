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

package org.seaborne.delta.link;

import java.util.Objects;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.Version;
import org.seaborne.patch.RDFPatch;

/**
 * Operations on a single patch log. This is a pairing of a {@link DeltaLink} (connection
 * to a patch server) and the reference to a specific log
 */
public class DeltaLog {
    
    private final DeltaLink dLink;
    private final Id dsRef;

    public DeltaLog(DeltaLink dLink, Id dsRef) {
        Objects.requireNonNull(dLink, "Argument dLink"); 
        Objects.requireNonNull(dsRef, "Argument dsRef"); 
        this.dLink = dLink;
        this.dsRef = dsRef;
    }
    
    public Version getCurrentVersion() {
        return dLink.getCurrentVersion(dsRef);
    }
    
    public RDFPatch fetch(Id patchId) {
        return dLink.fetch(dsRef, patchId); 
    }
    
    public RDFPatch fetch(Version version) {
        return dLink.fetch(dsRef, version); 
    }
    
    public Version append(RDFPatch patch) {
        return dLink.append(dsRef, patch);
    }

    public PatchLogInfo info() {
        return dLink.getPatchLogInfo(dsRef);
    }
    
    /** Return details of a patch log, or null if not registered. */
    public DataSourceDescription getDataSourceDescription() {
        return dLink.getDataSourceDescription(dsRef);
    }
}
