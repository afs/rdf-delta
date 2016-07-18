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

import java.util.LinkedList ;
import java.util.List ;

import org.apache.jena.atlas.lib.Lib ;
import org.apache.jena.graph.Node ;
import org.apache.jena.query.ReadWrite ;

/** Capture a stream of changes, then  play it to another {@link StreamChanges} */
public class StreamChangesBuffering implements StreamChanges {

    private List<Object> actions = new LinkedList<>() ; // ArrayList<>() ; // LinkedList better?
    
    public void play(StreamChanges target) {
        actions.forEach(a -> {
            if ( a instanceof AddQuad ) {
                AddQuad a2 = (AddQuad)a ;
                target.add(a2.g, a2.s, a2.p, a2.o) ;
                return ;
            }
            if ( a instanceof DeleteQuad ) {
                DeleteQuad a2 = (DeleteQuad)a ;
                target.delete(a2.g, a2.s, a2.p, a2.o) ;
                return ;
            }
            if ( a instanceof AddPrefix ) {
                AddPrefix a2 = (AddPrefix)a ;
                target.addPrefix(a2.gn, a2.prefix, a2.uriStr); 
                return ;
            }
            if ( a instanceof DeletePrefix ) {
                DeletePrefix a2 = (DeletePrefix)a ;
                target.deletePrefix(a2.gn, a2.prefix); 
                return ;
            }
            if ( a instanceof TxnBegin ) {
                target.txnBegin(((TxnBegin)a).mode) ;
                return ;
            }
            if ( a instanceof TxnPromote ) {
                target.txnPromote();
                return ;
            }
            if ( a instanceof TxnCommit ) {
                target.txnCommit();
                return ;
            }
            if ( a instanceof TxnAbort ) {
                target.txnAbort();
                return ;
            }
            System.err.println("Unrecognized action: "+Lib.className(a)+" : "+a) ;
        }) ;
        
    }
    
    private static class AddQuad {
        final Node g ;
        final Node s ;
        final Node p ;
        final Node o ;

        public AddQuad(Node g, Node s, Node p, Node o) {
            this.g = g ;
            this.s = s ;
            this.p = p ;
            this.o = o ;
        }
    }
    
    private static class DeleteQuad {
        final Node g ;
        final Node s ;
        final Node p ;
        final Node o ;

        public DeleteQuad(Node g, Node s, Node p, Node o) {
            this.g = g ;
            this.s = s ;
            this.p = p ;
            this.o = o ;
        }
    }
    
    // Tedious.

    private static class AddPrefix {
        final Node gn ;
        final String prefix ;
        final String uriStr ;

        public AddPrefix(Node gn, String prefix, String uriStr) {
            this.gn = gn ;
            this.prefix = prefix ;
            this.uriStr = uriStr ;
        }
    }

    private static class DeletePrefix {
        final Node gn ;
        final String prefix ;

        public DeletePrefix(Node gn, String prefix) {
            this.gn = gn ;
            this.prefix = prefix ;
        }
    }

    private static class SetBase {
        final String uriStr ;

        public SetBase(String uriStr) {
            this.uriStr = uriStr ;
        }
    }
    
    private static class TxnBegin {
        final ReadWrite mode ;

        public TxnBegin(ReadWrite mode) {
            this.mode = mode ;
        }
    }
    
    private static class TxnPromote { }
    
    private static class TxnCommit { }
    
    private static class TxnAbort { }

    public StreamChangesBuffering() { }

    private void collect(Object object) { 
        actions.add(object) ;
    }

    @Override
    public void start() {}

    @Override
    public void finish() {}

    public void reset() {
        actions.clear();
    }

    @Override
    public void add(Node g, Node s, Node p, Node o) {
        collect(new AddQuad(g, s, p, o)) ;
    }
    
    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        collect(new DeleteQuad(g, s, p, o)) ;
    }
    
    @Override
    public void addPrefix(Node gn, String prefix, String uriStr) {
        collect(new AddPrefix(gn, prefix, uriStr)) ;
    }
    
    
    @Override
    public void deletePrefix(Node gn, String prefix) {
        collect(new DeletePrefix(gn, prefix)) ;    
    }
    
    @Override
    public void setBase(String uriStr) {
        collect(new SetBase(uriStr)) ;
    }
    
    @Override
    public void txnBegin(ReadWrite mode) {
        collect(new TxnBegin(mode)) ;
    }
    
    @Override
    public void txnPromote() {
        collect(new TxnPromote()) ;
    }
    
    @Override
    public void txnCommit() {
        collect(new TxnCommit()) ;
    }
    
    @Override
    public void txnAbort() {
        collect(new TxnAbort()) ;
    }
}
