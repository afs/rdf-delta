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

import java.util.Set;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.ext.com.google.common.base.Objects;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.system.Txn;
import org.junit.Test;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesCollector;

/** Test a client connection over a link */  
public abstract class AbstractTestDeltaConnection {
    
    public abstract Setup.LinkSetup getSetup();
    public DeltaLink getLink() { return getSetup().getLink(); }
    
    protected DeltaConnection connect() {
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        return connect(dsg, 0);
    }
    
    protected DeltaConnection connect(DatasetGraph shadow, int localVersion) {
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        Id dsRef = dLink.newDataSource("foo", "http://example/datasource");
        DeltaConnection dConn = DeltaConnection.create("label",
                                                       clientId, dsRef,
                                                       shadow,
                                                       dLink);
        return dConn;
    }

    // Make a change, ensure the local dataset is changed. 
    @Test
    public void change_01() {
        DeltaConnection dConn = connect();
        
        long verLocal0 = dConn.getLocalVersionNumber();
        long verRemotel0 = dConn.getRemoteVersionLatest();
        
        DatasetGraph dsg = dConn.getDatasetGraph();
        Txn.executeWrite(dsg, ()->{
            dsg.add(SSE.parseQuad("(:gx :sx :px :ox)"));
        });
        
        int verLocal1 = dConn.getLocalVersionNumber();
        int verRemotel1 = dConn.getRemoteVersionLatest();
        assertEquals(verLocal1, dConn.getLocalVersionNumber());
        assertEquals(verRemotel1, dConn.getRemoteVersionLatest());
        
        assertFalse(dConn.getDatasetGraph().isEmpty());
        if ( dConn.getStorage() != null )
            assertFalse(dConn.getStorage().isEmpty());
    }

    //Make a change, get the patch, apply to a clean dsg. Are the datasets the same?
    @Test
    public void change_02() {
        DeltaConnection dConn = connect();
        
        Id dsRef = dConn.getDatasourceId();
        int version = dConn.getRemoteVersionLatest();

        dConn.getLocalVersionNumber();
        dConn.getRemoteVersionNumber();
        dConn.getRemoteVersionLatest();
        
        DatasetGraph dsg = dConn.getDatasetGraph();
        Txn.executeWrite(dsg, ()->{
            Quad q = SSE.parseQuad("(_ :s1 :p1 :o1)");
            dsg.add(q);
        });
        
        DatasetGraph dsg2 = DatasetGraphFactory.createTxnMem();
        int ver = dConn.getRemoteVersionLatest();
        RDFPatch patch1 = dConn.getLink().fetch(dsRef, ver) ;
        RDFPatchOps.applyChange(dsg2, patch1);
        
        Set<Quad> set1 = Txn.calculateRead(dsg, ()->Iter.toSet(dsg.find()));
        Set<Quad> set2 = Txn.calculateRead(dsg2, ()->Iter.toSet(dsg2.find()));
        assertEquals(set1, set2);
    }

    private static boolean equals(RDFPatch patch1, RDFPatch patch2) {
        RDFChangesCollector c1 = new RDFChangesCollector();
        // The getRDFPatch is a RDFPatchStored which supports hashCode and equals.
        RDFChangesCollector.RDFPatchStored p1 = (RDFChangesCollector.RDFPatchStored)c1.getRDFPatch();
        
        RDFChangesCollector c2 = new RDFChangesCollector();
        RDFChangesCollector.RDFPatchStored p2 = (RDFChangesCollector.RDFPatchStored)c2.getRDFPatch();
        
        return Objects.equal(p1, p2);
    }
}
