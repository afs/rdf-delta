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

import org.seaborne.delta.Id;
import org.seaborne.delta.Version;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch;

/**
 * A {@link DeltaLinkListener} that listens for patch fetch requests on the
 * {@link DeltaLink} and plays the retrieved patch to another {@link RDFChanges}. The
 * callback happens before the fetch request is delivered to the caller so the patch will
 * not have been applied yet.
 */
public class DeltaLinkPatchEvents implements DeltaLinkListener {
    private final Id         target;
    private final RDFChanges replay;

    public DeltaLinkPatchEvents(Id dsRef, RDFChanges replay) {
        this.target = Objects.requireNonNull(dsRef);
        this.replay = Objects.requireNonNull(replay);
    }

    @Override
    public void fetchById(Id dsRef, Id patchId, RDFPatch patch) {
        if ( target.equals(dsRef) )
            replay(patch);
    }

    @Override
    public void fetchByVersion(Id dsRef, Version version, RDFPatch patch) {
        if ( target.equals(dsRef) )
            replay(patch);
    }

    private void replay(RDFPatch patch) {
        patch.apply(replay);
    }
}