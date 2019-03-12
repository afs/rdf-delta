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

package org.seaborne.delta.server.patchstores;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.jena.atlas.lib.ListUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.seaborne.delta.Id;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public abstract class AbstractTestPatchStorage {
    
    protected abstract PatchStorage patchStorage();
    
    @Test public void patchStorage_1_empty() {
        PatchStorage patchStorage = patchStorage();
        assertFalse(patchStorage.find().findAny().isPresent());

        Id id = Id.create();
        RDFPatch patch1 = patchStorage.fetch(id);
        assertNull(patch1);
    }
    
    @Test public void patchStorage_2_singlePatch() {
        PatchStorage patchStorage = patchStorage();
        RDFPatch patch = RDFPatchOps.emptyPatch();
        Id id = Id.create();
        patchStorage.store(id, patch);

        RDFPatch patch1 = patchStorage.fetch(id);
        assertNotNull(patch1);
        assertEquals(patch.getId(), patch1.getId());
        
        List<Id> x = ListUtils.toList(patchStorage.find());
        assertFalse(x.isEmpty());
    }
    
    @Test public void patchStorage_3_twoPatches() {
        PatchStorage patchStorage = patchStorage();
        RDFPatch patch1 = RDFPatchOps.emptyPatch();
        Id id1 = Id.create();
        Id id2 = Id.create();
        patchStorage.store(id1, patch1);

        RDFPatch patch1f = patchStorage.fetch(id1);
        assertNotNull(patch1);
        assertEquals(patch1.getId(), patch1f.getId());

        RDFPatch patch2f = patchStorage.fetch(id2);
        assertNull(patch2f);
        
        RDFPatch patch2 = RDFPatchOps.emptyPatch();
        patchStorage.store(id2, patch2);
        
        patch2f = patchStorage.fetch(id2);
        assertNotNull(patch2f);
        assertEquals(patch2.getId(), patch2f.getId());
        assertNotEquals(patch1.getId(), patch2f.getId());
        assertNotEquals(patch1f.getId(), patch2f.getId());
        
        RDFPatch patch1f_a = patchStorage.fetch(id1);
        assertNotNull(patch1f_a);
        assertEquals(patch1f_a.getId(), patch1f_a.getId());
    }
}
