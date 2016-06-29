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

package org.seaborne.delta.base;

import org.apache.jena.graph.Node ;
import org.apache.jena.query.ReadWrite ;

public class StreamChanges2 implements StreamChanges
{
    private final StreamChanges changes1 ;
    private final StreamChanges changes2 ;
    
    public StreamChanges2(StreamChanges changes1, StreamChanges changes2) {
        this.changes1 = changes1 ;
        this.changes2 = changes2 ;
    }
    
    @Override
    public void start() {
        changes1.start();
        changes2.start();
    }

    @Override
    public void finish() {
        changes1.finish();
        changes2.finish();
    }
    
    @Override
    public void add(Node g, Node s, Node p, Node o) {
        changes1.add(g, s, p, o);
        changes2.add(g, s, p, o);
    }

    @Override
    public void delete(Node g, Node s, Node p, Node o) { 
        changes1.delete(g, s, p, o);
        changes2.delete(g, s, p, o);
    }
    
    @Override
    public void addPrefix(Node graph, String prefix, String uriStr) {
        changes1.addPrefix(graph, prefix, uriStr);
        changes2.addPrefix(graph, prefix, uriStr);
    } 
    
    @Override
    public void deletePrefix(Node graph, String prefix) {
        changes1.deletePrefix(graph, prefix);
        changes2.deletePrefix(graph, prefix);
    }
    
    @Override
    public void setBase(String uriStr) {
        changes1.setBase(uriStr);
        changes2.setBase(uriStr);
    }

    @Override
    public void txnBegin(ReadWrite mode) {
        changes1.txnBegin(mode);
        changes2.txnBegin(mode);
    }
    
    @Override
    public void txnPromote() {
        changes1.txnPromote();
        changes2.txnPromote();
    }
    
    @Override
    public void txnCommit() {
        changes1.txnCommit();
        changes2.txnCommit();
    }
    
    @Override
    public void txnAbort() {
        changes1.txnAbort();
        changes2.txnAbort();
    }
}
