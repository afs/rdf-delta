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

package org.seaborne.delta.changes;

import org.apache.jena.graph.Node ;
import org.apache.jena.query.ReadWrite ;

public class StreamChangesCounter implements StreamChanges {
    
    public long countAddQuad      = 0 ;
    public long countDeleteQuad   = 0 ;
    public long countAddPrefix    = 0 ;
    public long countDeletePrefix = 0 ;

    public StreamChangesCounter() {}
    
    @Override
    public void start() {}

    @Override
    public void finish() {}

    @Override
    public void add(Node g, Node s, Node p, Node o) {
        countAddQuad++ ;
    }

    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        countDeleteQuad++ ;
    }

    @Override
    public void addPrefix(Node gn, String prefix, String uriStr) {
        countAddPrefix++ ;
    }

    @Override
    public void deletePrefix(Node gn, String prefix) {
        countDeletePrefix++ ;
    }

    @Override
    public void setBase(String uriStr) {}

    @Override
    public void txnBegin(ReadWrite mode) {}

    @Override
    public void txnPromote() {}

    @Override
    public void txnCommit() {}

    @Override
    public void txnAbort() {}

}
