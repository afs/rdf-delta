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

import java.io.InputStream;

import org.apache.jena.atlas.io.IO;
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
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;

/** Test a client connection over a link */  
public abstract class AbstractTestDeltaConnection {
    
    public abstract DeltaLink getLink();

    public abstract void reset();
    public abstract Id getDataSourceId();
    
    protected static String DIR = "testing/Delta";
    protected static String TMP = "target/Delta";
    static {
        FileOps.ensureDir(TMP);
    }
    
//    protected static UUID uuid1 = UUID.randomUUID();
//    protected static Id id1 = Id.fromUUID(uuid1);
//    protected static UUID uuid2 = UUID.randomUUID();
//    protected static Id id2 = Id.fromUUID(uuid2);
//    
    @Before public void beforeTest() { reset(); }
    @After  public void afterTest() {}
    
    public DeltaConnection connect(DeltaLink link) {
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        return connect(link, dsg, 0);
    }
    
    public DeltaConnection connect(DeltaLink dLink, DatasetGraph shadow, int localVersion) {
        Id clientId = Id.create();
        DeltaConnection dConn = DeltaConnection.create("label",
                                                       clientId, getDataSourceId(),
                                                       shadow,
                                                       dLink);
        return dConn;
    }

    @Test
    public void patch_01() {
        DeltaLink dLink = getLink();
        DeltaConnection dConn = connect(getLink());
        
        Id id1 = Id.create();
        RegToken regToken = dLink.register(id1);
        
        InputStream in = IO.openFile(DIR+"/patch1.rdfp");
        RDFPatch patch = RDFPatchOps.read(in);
        
        int verLocal0 = dConn.getLocalVersionNumber();
        int verRemotel0 = dConn.getRemoteVersionLatest();
        dConn.sendPatch(patch);
        assertEquals(verLocal0+1, dConn.getLocalVersionNumber());
        assertEquals(verRemotel0+1, dConn.getRemoteVersionLatest());
        
        assertFalse(dConn.getDatasetGraph().getDefaultGraph().isEmpty());
        if ( dConn.getStorage() != null )
            assertFalse(dConn.getStorage().getDefaultGraph().isEmpty());
    }
}
