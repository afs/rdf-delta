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

import org.junit.Test;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.Id;
import org.seaborne.delta.LogEntry;
import org.seaborne.delta.Version;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;

/**
 * Some basic test to make sure a patch log works.
 * Most testing comes from using in the integration tests.
 */

public abstract class AbstractTestPatchLogIndex {

    private static Version version_1 = Version.create(1);
    private static Version version_2 = Version.create(2);
    private static Version version_3 = Version.create(3);

    protected abstract PatchLogIndex patchLogIndex();

    @Test
    public void patchLogIndex_1_empty() {
        PatchLogIndex patchLogIdx = patchLogIndex();

        assertTrue(patchLogIdx.isEmpty());

        assertNull(patchLogIdx.getPatchInfo(Id.create()));
        assertNull(patchLogIdx.getCurrentId());
        assertNull(patchLogIdx.getPreviousId());
        assertEquals(Version.INIT, patchLogIdx.getCurrentVersion());
    }

    @Test
    public void patchLogIndex_2_singlePatch() {
        PatchLogIndex patchLogIdx = patchLogIndex();
        assertTrue(patchLogIdx.isEmpty());

        RDFPatch patch = RDFPatchOps.emptyPatch();
        Id id1 = Id.fromNode(patch.getId());
        Id prev1 = Id.fromNode(patch.getPrevious());
        Version ver1 = patchLogIdx.nextVersion();
        assertEquals(version_1, ver1);
        patchLogIdx.save(ver1, id1, Id.fromNode(patch.getPrevious()));

        LogEntry entry = patchLogIdx.getPatchInfo(id1);
        assertEquals(ver1, entry.getVersion());
        assertEquals(id1, entry.getPatchId());
        assertEquals(prev1, entry.getPrevious());
        assertFalse(patchLogIdx.isEmpty());

        assertEquals(id1, patchLogIdx.getEarliestId());
        assertEquals(ver1, patchLogIdx.getEarliestVersion());
        assertEquals(id1,patchLogIdx.getCurrentId());
        assertEquals(ver1, patchLogIdx.getCurrentVersion());
    }

    @Test
    public void patchLogIndex_3_two_patches() {
        PatchLogIndex patchLogIdx = patchLogIndex();
        assertTrue(patchLogIdx.isEmpty());

        RDFPatch patch1 = RDFPatchOps.emptyPatch();
        Id id1 = Id.fromNode(patch1.getId());
        Id prev1 = Id.fromNode(patch1.getPrevious());   // null.
        Version ver1 = patchLogIdx.nextVersion();
        assertEquals(version_1, ver1);
        patchLogIdx.save(ver1, id1, prev1);

        RDFPatch patch2 = RDFPatchOps.emptyPatch();
        Id id2 = Id.fromNode(patch2.getId());
        Id prev2 = id1;// Id.fromNode(patch2.getPrevious());
        Version ver2 = patchLogIdx.nextVersion();
        assertEquals(version_2, ver2);
        patchLogIdx.save(ver2, id2, id1);

        assertNotEquals(ver1, ver2);
        assertNotEquals(id1, id2);
        assertNotEquals(prev1, prev2);

        LogEntry entry1 = patchLogIdx.getPatchInfo(id1);
        assertEquals(ver1,  entry1.getVersion());
        assertEquals(id1,   entry1.getPatchId());
        assertEquals(prev1, entry1.getPrevious());

        assertEquals(id1,  patchLogIdx.getEarliestId());
        assertEquals(ver1, patchLogIdx.getEarliestVersion());
        assertEquals(id2,  patchLogIdx.getCurrentId());
        assertEquals(ver2, patchLogIdx.getCurrentVersion());

        LogEntry entry2 = patchLogIdx.getPatchInfo(id2);
        assertEquals(ver2,  entry2.getVersion());
        assertEquals(id2,   entry2.getPatchId());
        assertEquals(id1, entry2.getPrevious());
        assertFalse(patchLogIdx.isEmpty());

        assertNotEquals(id2, patchLogIdx.getEarliestId());
        assertNotEquals(ver2, patchLogIdx.getEarliestVersion());

        assertEquals(id2, patchLogIdx.getCurrentId());
        assertEquals(ver2, patchLogIdx.getCurrentVersion());
    }

    @Test(expected=NullPointerException.class)
    public void patchLogIndex_4_bad_save() {
        PatchLogIndex patchLogIdx = patchLogIndex();
        assertTrue(patchLogIdx.isEmpty());
        patchLogIdx.save(null, null, null);
    }

    @Test(expected=NullPointerException.class)
    public void patchLogIndex_5_bad_save() {
        PatchLogIndex patchLogIdx = patchLogIndex();
        assertTrue(patchLogIdx.isEmpty());
        patchLogIdx.save(version_1, null, null);
    }

    @Test(expected=DeltaException.class)
    public void patchLogIndex_6_bad_save() {
        PatchLogIndex patchLogIdx = patchLogIndex();
        assertTrue(patchLogIdx.isEmpty());
        Id id1 = Id.create();
        Id id2 = Id.create();
        patchLogIdx.save(version_1, id1, id2);
    }

    @Test(expected=DeltaException.class)
    public void patchLogIndex_7_bad_save() {
        PatchLogIndex patchLogIdx = patchLogIndex();
        assertTrue(patchLogIdx.isEmpty());
        Id id1 = Id.create();
        patchLogIdx.save(version_1, id1, null);
        patchLogIdx.save(version_1, id1, null);
    }

    @Test(expected=DeltaException.class)
    public void patchLogIndex_8_bad_save() {
        PatchLogIndex patchLogIdx = patchLogIndex();
        assertTrue(patchLogIdx.isEmpty());
        Id id1 = Id.create();
        Id id2 = Id.create();
        patchLogIdx.save(version_1, id1, null);
        patchLogIdx.save(version_1, id2, id1);
    }

    @Test(expected=DeltaException.class)
    public void patchLogIndex_9_bad_save() {
        PatchLogIndex patchLogIdx = patchLogIndex();
        assertTrue(patchLogIdx.isEmpty());
        Id id1 = Id.create();
        Id id2 = Id.create();
        Id id3 = Id.create();
        patchLogIdx.save(version_1, id1, null);
        patchLogIdx.save(version_2, id2, id1);
        patchLogIdx.save(version_1, id3, id2);
    }
}
