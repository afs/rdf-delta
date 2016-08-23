/**
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

import org.apache.jena.graph.Node ;
import org.apache.jena.query.Dataset ;
import org.apache.jena.query.DatasetFactory ;
import org.apache.jena.query.ReadWrite ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.DatasetGraphFactory ;
import org.apache.jena.sparql.core.Quad ;
import org.apache.jena.sparql.sse.SSE ;
import org.apache.jena.system.Txn ;
import org.junit.Assert ;
import org.junit.Test ;
import org.seaborne.delta.base.DatasetGraphBuffering ;

/** Tests for a transactional dataset with added abort capability. */ 
public class TestDatasetGraphWithAbort //extends AbstractTestTransactionLifecycle
{
    static Quad q1 = SSE.parseQuad("(:g1 :s1 :p1 :o1)") ;
    static Quad q2 = SSE.parseQuad("(_ :s1 :p1 :o1)") ;
    static Quad q3 = SSE.parseQuad("(:g2 :s1 :p1 :o1)") ;
    static Node s1 = SSE.parseNode(":s1") ;   
    
    //@Override
    protected Dataset create() {
        DatasetGraph dsg1 = DatasetGraphFactory.create() ;
        DatasetGraphBuffering dsg = new DatasetGraphBuffering(dsg1) ;
        return DatasetFactory.wrap(dsg) ;
    }
    
    @Test public void abort_data_1() {
        DatasetGraph dsg = create().asDatasetGraph() ;
        Txn.execWrite(dsg, ()->dsg.add(q1)) ;
        Assert.assertTrue(dsg.contains(q1)) ;
        Assert.assertFalse(dsg.contains(q2)) ;
        dsg.begin(ReadWrite.WRITE);
        dsg.add(q2) ;
        dsg.abort();
        dsg.end();
        Assert.assertTrue(dsg.contains(q1)) ;
        Assert.assertFalse(dsg.contains(q2)) ;
    }
}
