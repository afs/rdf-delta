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

package test;

import static org.junit.Assert.*;

import org.junit.Test;
import org.seaborne.delta.server.local.PatchLog;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;

public abstract class AbstractTestPatchLog {
    
    protected abstract PatchLog patchLog();
    
    @Test public void ps1() {
        PatchLog patchLog = patchLog();
        
        boolean b = patchLog.isEmpty();
        long x = patchLog.getEarliestVersion();
        
        assertTrue(patchLog.isEmpty());
    }
    
    @Test public void ps2() {
        PatchLog patchLog = patchLog();
        assertTrue(patchLog.isEmpty());
        RDFPatch patch = RDFPatchOps.emptyPatch();
        patchLog.append(patch);
        assertFalse(patchLog.isEmpty());
    }
    
    @Test public void ps3() {
        PatchLog patchLog = patchLog();
        RDFPatch patch = RDFPatchOps.emptyPatch();
        long v = patchLog.append(patch);
        assertEquals(1, v);
        
        RDFPatch patch1 = patchLog.fetch(1);
        assertNotNull(patch1);
        RDFPatch patch2 = patchLog.fetch(2);
        assertNull(patch2);
    }

    @Test public void ps4() {
        PatchLog patchLog = patchLog();
        RDFPatch patch = RDFPatchOps.emptyPatch();
        patchLog.append(patch);
        // Does not exist
        RDFPatch patch2 = patchLog.fetch(2);
        assertNull(patch2);
    }

}
