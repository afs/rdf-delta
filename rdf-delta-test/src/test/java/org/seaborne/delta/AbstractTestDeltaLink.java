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

package org.seaborne.delta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.Id;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.system.DatasetGraphChanges;

/** Tests for the link (multiplexex connection to the server or local engine) */
public abstract class AbstractTestDeltaLink {
    
    public abstract DeltaLink getLink();

    public abstract void reset();
    public abstract Id getDataSourceId();
    
    protected static String DIR = "testing/Delta";
    protected static String TMP = "target/Delta";
    static {
        FileOps.ensureDir(TMP);
    }
    
    protected static UUID uuid1 = UUID.randomUUID();
    protected static Id id1 = Id.fromUUID(uuid1);
    protected static UUID uuid2 = UUID.randomUUID();
    protected static Id id2 = Id.fromUUID(uuid2);
    
    @Before public void beforeTest() { reset(); }
    @After  public void afterTest() {}
    
    @Test
    public void register_01() {
        DeltaLink dLink = getLink();
        RegToken regToken = dLink.register(id2);
    }
    
    @Test
    public void register_02() { 
        DeltaLink dLink = getLink();
        RegToken regToken = dLink.register(id1);
        assertTrue(dLink.isRegistered(id1));
        assertFalse(dLink.isRegistered(id2));
        assertTrue(dLink.isRegistered(regToken));
        dLink.deregister(regToken);
        assertFalse(dLink.isRegistered(id1));
        assertFalse(dLink.isRegistered(id2));
        assertFalse(dLink.isRegistered(regToken));
    }
    
    @Test
    public void client_01() {
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        DeltaConnection dConn = DeltaConnection.create("label",
                                                       clientId, getDataSourceId(),
                                                       dsg,
                                                       dLink);
        assertNotNull(dConn.getName());
        assertEquals(0, dConn.getLocalVersionNumber());
        assertEquals(0, dConn.getRemoteVersionNumber());
        assertEquals(0, dConn.getRemoteVersionLatest());
        assertNotNull(dConn.getStorage());

        // Check the DSG  
        assertEquals(dsg, dConn.getStorage());
        DatasetGraph dsg1 = dConn.getDatasetGraph();
        assertNotEquals(dsg, dsg1);
        assertTrue(dsg1 instanceof DatasetGraphChanges);
    }
}
