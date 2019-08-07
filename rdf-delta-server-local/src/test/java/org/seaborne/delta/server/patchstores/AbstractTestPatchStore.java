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

import org.junit.*;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.server.local.DPS;
import org.seaborne.delta.server.local.DataRegistry;
import org.seaborne.delta.server.local.PatchLog;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.system.DeltaSystem;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public abstract class AbstractTestPatchStore {

    // XXX Convert to parameterized tests??

    private static int counter = 0;
    private PatchStore patchStore = null;

    @BeforeClass public static void setup() {
        DeltaSystem.init();
    }

    @Before public void setupTest() {
        patchStore = provider();
    }

    @After public void teardown() {
        patchStore = null;
    }

    /**
     * Return the PatchStore implementation under test. Return the same object each time.
     */
    private PatchStore provider() {
        if ( patchStore == null ) {
            DataRegistry dataRegistry = new DataRegistry(this.getClass().getName()+":"+(++counter));
            patchStore = patchStore(dataRegistry);
        }
        return patchStore;
    }

    protected abstract PatchStore patchStore(DataRegistry dataRegistry);

    @Test public void patchStore_0() {
        PatchStore ps = provider();
        DataRegistry dataRegistry = ps.getDataRegistry();
        assertTrue(dataRegistry.isEmpty());
        assertTrue(ps.listDataSources().isEmpty());
    }

    @Test public void patchStore_1() {
        PatchStore ps = provider();
        DataRegistry dataRegistry = ps.getDataRegistry();

        DataSourceDescription dsdSetup = new DataSourceDescription(Id.create(), "ABC", "http://example/ABC");
        PatchLog patchLog = ps.createLog(dsdSetup);
        Id logId = patchLog.getLogId();

        assertEquals(dsdSetup.getId(), logId);
        assertNotNull(dataRegistry.getByName("ABC"));
        assertFalse(dataRegistry.isEmpty());
        assertTrue(ps.logExists(dsdSetup.getId()));
        assertEquals(patchLog, ps.getLog(logId));
    }

    @Test public void patchStore_2() {
        PatchStore ps = provider();
        DataRegistry dataRegistry = ps.getDataRegistry();

        DataSourceDescription dsdSetup = new DataSourceDescription(Id.create(), "ABC", "http://example/ABC");
        PatchLog patchLog = ps.createLog(dsdSetup);
        assertEquals(dsdSetup.getId(), patchLog.getLogId());
        Id logId = patchLog.getLogId();
        assertFalse(dataRegistry.isEmpty());
        assertNotNull(dataRegistry.getByName("ABC"));
        assertTrue(ps.logExists(dsdSetup.getId()));
        assertNotNull(ps.getLog(logId));

        ps.release(patchLog);
        assertTrue(dataRegistry.isEmpty());
        assertNull(dataRegistry.getByName("ABC"));
        assertFalse(ps.logExists(dsdSetup.getId()));
        assertNull(ps.getLog(logId));
    }

    // Recovery (does not apply to PatchStoreMem)
    //@Test
    public void recovery1() {
        PatchStore ps = provider();

        // Match dsd2 below
        DataSourceDescription dsdSetup = new DataSourceDescription(Id.create(), "ABC", "http://example/ABC");
        PatchLog patchLog = ps.createLog(dsdSetup);

        RDFPatch patch = RDFPatchOps.emptyPatch();
        patchLog.append(patch);

        PatchLogInfo info = patchLog.getInfo();
        Id id = patchLog.getLogId();
        DataSourceDescription dsd = patchLog.getDescription();

        DPS.resetSystem();

        String name = dsd.getName();
        PatchStore provider = provider();

        // Same FileStore, different PatchLog?
        DataSourceDescription dsd2 = new DataSourceDescription(id, name, "http://example/ABC");
        PatchLog patchLog1 = provider.connectLog(dsd2);
        PatchLogInfo info1 = patchLog1.getInfo();
        assertEquals(info, info1);
    }

    // MORE TESTS
    // Get non-existent.
}
