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
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.Version;
import org.seaborne.delta.server.local.PatchLog;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.rdfpatch.RDFPatchOps;

/**
 * Some basic test to make sure a patch log works.
 * Most testing comes from using in the integration tests.
 */

public abstract class AbstractTestPatchLog {
    //Parameterize tests
    private static Version version_1 = Version.create(1);
    private static Version version_2 = Version.create(2);
    private static Version version_3 = Version.create(3);

    protected abstract PatchLog patchLog();

    @Test
    public void patchLog_1_empty() {
        PatchLog patchLog = patchLog();

        boolean b = patchLog.isEmpty();
        assertTrue(patchLog.isEmpty());

        PatchLogInfo x = patchLog.getInfo();
        assertEquals(null, x.getLatestPatch());
        assertEquals(Version.INIT, x.getMaxVersion());
        assertEquals(Version.INIT, x.getMinVersion());
    }

    @Test public void patchLog_2_singlePatch() {
        PatchLog patchLog = patchLog();
        assertTrue(patchLog.isEmpty());

        RDFPatch patch = RDFPatchOps.emptyPatch();
        Version v = patchLog.append(patch);
        assertEquals(version_1, v);
        assertFalse(patchLog.isEmpty());

        RDFPatch patch1 = patchLog.fetch(version_1);
        assertNotNull(patch1);

        PatchLogInfo x = patchLog.getInfo();
        assertEquals(patch.getId(), x.getLatestPatch().asNode());

        x.getMaxVersion();

        assertEquals(Version.FIRST, x.getMaxVersion());
        assertEquals(Version.FIRST, x.getMinVersion());

        RDFPatch patch2 = patchLog.fetch(version_2);
        assertNull(patch2);
    }

    @Test
    public void patchLog_3_two_patches() {
        PatchLog patchLog = patchLog();
        RDFPatch patchAdd1 = RDFPatchOps.emptyPatch();
        Version v1 = patchLog.append(patchAdd1);
        assertEquals(version_1, v1);

        // Does not exist
        RDFPatch patch_missing = patchLog.fetch(version_2);
        assertNull(patch_missing);

        RDFPatch patchFetch1 = patchLog.fetch(version_1);
        assertNotNull(patchFetch1);

        RDFPatch patchAdd2 = RDFPatchOps.withHeader(patchAdd1, Id.create().asNode(), patchAdd1.getId());

        Version v2 = patchLog.append(patchAdd2);
        assertEquals(version_2, v2);

        RDFPatch patch3 = patchLog.fetch(version_3);
        assertNull(patch3);

        RDFPatch patchFetch2 = patchLog.fetch(version_2);
        assertNotNull(patchFetch2);

        PatchLogInfo x = patchLog.getInfo();
        assertEquals(patchFetch2.getId(), x.getLatestPatch().asNode());
        assertEquals(version_2, x.getMaxVersion());
        assertEquals(Version.FIRST, x.getMinVersion());
    }
}
