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

package org.seaborne.patch;

import org.apache.jena.graph.Node ;

public interface RDFChanges {
    // Changes lifecycle.
    // Must not span txn boundaries.
    public void start() ;
    public void finish() ;
    
    public void header(String field, Node value) ; 
    
//    public void add(Node s, Node p, Node o) ;
//    public void delete(Node s, Node p, Node o) ;
    
    // g = null for Triple,
    // or "urn:x-arq:DefaultGraph" or "urn:x-arq:DefaultGraphNode"

    public void add(Node g, Node s, Node p, Node o) ;
    public void delete(Node g, Node s, Node p, Node o) ;
    
    public void addPrefix(Node gn, String prefix, String uriStr) ; 
    public void deletePrefix(Node gn, String prefix) ;
    
    /** Indicator that a transaction begins */
    public void txnBegin() ;
    
    /** Indicator that a transaction commits */
    public void txnCommit() ;
    
    /** Indicator that a transaction aborts */
    public void txnAbort() ;
    
}
