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

package org.seaborne.patch.changes;

import org.apache.jena.graph.Node;
import org.seaborne.patch.RDFChanges;

public class RDFChangesCounter implements RDFChanges {

    public long countStart        = 0;
    public long countFinish       = 0;
    public long countHeader       = 0;
    public long countAddData      = 0;
    public long countDeleteData   = 0;
    public long countAddPrefix    = 0;
    public long countDeletePrefix = 0;
    public long countTxnBegin     = 0;
    public long countTxnCommit    = 0;
    public long countTxnAbort     = 0;

    public RDFChangesCounter() {}
    
    public void reset() {
        countStart        = 0;
        countFinish       = 0;
        countHeader       = 0;
        countAddData      = 0;
        countDeleteData   = 0;
        countAddPrefix    = 0;
        countDeletePrefix = 0;
        countTxnBegin     = 0;
        countTxnCommit    = 0;
        countTxnAbort     = 0;
    }

    @Override
    public void start() {
        countStart++;
    }

    @Override
    public void finish() {
        countFinish++;
    }

    @Override
    public void header(String field, Node value) {
        countHeader++;
    }

    @Override
    public void add(Node g, Node s, Node p, Node o) {
        countAddData++;
    }

    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        countDeleteData++;
    }

    @Override
    public void addPrefix(Node gn, String prefix, String uriStr) {
        countAddPrefix++;
    }

    @Override
    public void deletePrefix(Node gn, String prefix) {
        countDeletePrefix++;
    }

    @Override
    public void txnBegin() {
        countTxnBegin++;
    }

    @Override
    public void txnCommit() {
        countTxnCommit++;
    }

    @Override
    public void txnAbort() {
        countTxnAbort++;
    }
}
