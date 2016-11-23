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

import static org.seaborne.patch.ChangeCode.ADD_PREFIX ;
import static org.seaborne.patch.ChangeCode.ADD_QUAD ;
import static org.seaborne.patch.ChangeCode.DEL_PREFIX ;
import static org.seaborne.patch.ChangeCode.DEL_QUAD ;
import static org.seaborne.patch.ChangeCode.SET_BASE ;

import org.apache.jena.graph.Node ;
import org.apache.jena.sparql.core.Quad ;
import org.seaborne.patch.ChangeCode ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.riot.tio.TokenWriter ;

/**
 * Write out a changes as a stream of syntax tokens.
 * The provided {@link TokenWriter} determines the concrete syntax - binary or text. 
 */
public class RDFChangesWriter implements RDFChanges {
    private final TokenWriter tok ;

    public RDFChangesWriter(TokenWriter out) {
        this.tok = out ;
    }
    
    @Override
    public void start() {
    }
    
    @Override
    public void finish() { }

    @Override
    public void header(String field, Node value) {
        tok.startTuple();
        tok.sendWord("H");
        tok.sendWord(field);
        output(value);
        tok.endTuple();
    }
    
    public void flush() {
        tok.flush(); 
    }
    
    @Override
    public void add(Node g, Node s, Node p, Node o) {
        output(ADD_QUAD, g, s, p, o) ;
    }

    private void output(ChangeCode change, Node g, Node s, Node p, Node o) {
        tok.startTuple();
        output(change) ;
        output(s) ;
        output(p) ;
        output(o) ;
        if ( g != null && ! Quad.isDefaultGraph(g) )
            output(g) ;
        tok.endTuple();
        tok.flush();
    }

    private void output(Node g) {
        tok.sendNode(g); 
    }

    private void output(ChangeCode code) {
        tok.sendWord(code.label);
    }

    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        output(DEL_QUAD, g, s, p, o) ;
    }

    @Override
    public void addPrefix(Node gn, String prefix, String uriStr) {
        tok.startTuple();
        output(ADD_PREFIX);
        tok.sendString(prefix);
        tok.sendString(uriStr) ;
        if ( gn != null )
            tok.sendNode(gn);
        tok.endTuple();
        tok.flush();
    }

    @Override
    public void deletePrefix(Node gn, String prefix) {
        tok.startTuple();
        output(DEL_PREFIX);
        tok.sendString(prefix);
        if ( gn != null )
            tok.sendNode(gn);
        tok.endTuple();
        tok.flush();
    }

    @Override
    public void setBase(String uriStr) {
        tok.startTuple();
        output(SET_BASE) ;
        tok.sendString(uriStr);
        tok.endTuple();
        tok.flush();
    }

    private void oneline(ChangeCode code) {
        tok.startTuple();
        tok.sendWord(code.label);
        tok.endTuple();
        tok.flush() ;
    }
    
    @Override
    public void txnBegin() {
        oneline(ChangeCode.TXN_BEGIN) ;
    }

    @Override
    public void txnCommit() {
        oneline(ChangeCode.TXN_COMMIT) ;
    }

    @Override
    public void txnAbort() {
        oneline(ChangeCode.TXN_ABORT) ;
    }
}
