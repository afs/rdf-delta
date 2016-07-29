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

import java.util.LinkedList ;
import java.util.List ;

import org.apache.jena.atlas.lib.Lib ;
import org.apache.jena.graph.Node ;
import org.apache.jena.query.ReadWrite ;
import org.seaborne.delta.lib.ListIteratorReverse ;
import org.seaborne.patch.items.* ;

/** Capture a stream of changes, then play it to another {@link RDFChanges} */
public class RDFChangesCollector implements RDFChanges {

    private List<ChangeItem> actions = new LinkedList<>() ; // ArrayList<>() ; // LinkedList better?
    
    /** Play forwards */
    public void play(RDFChanges target) {
        actions.forEach(a -> enact(a, target)) ;
    }

    /** Play backwards, swapping adds for deletes and delete for adds */
    public void playReverse(RDFChanges target) {
        System.err.println("playReverse: Partially implemented") ;
        // More complicated - turn into transaction chunks then ... 
        
        ListIteratorReverse.reverse(actions.listIterator()).forEachRemaining(a-> enactFlip(a, target)) ;
    }
                            
    private void enactFlip(ChangeItem a, RDFChanges target) {
        if ( a instanceof AddQuad ) {
            AddQuad a2 = (AddQuad)a ;
            target.delete/*add*/(a2.g, a2.s, a2.p, a2.o) ;
            return ;
        }
        if ( a instanceof DeleteQuad ) {
            DeleteQuad a2 = (DeleteQuad)a ;
            target.add/*delete*/(a2.g, a2.s, a2.p, a2.o) ;
            return ;
        }
//        if ( a instanceof AddPrefix ) {
//            AddPrefix a2 = (AddPrefix)a ;
//            target.addPrefix(a2.gn, a2.prefix, a2.uriStr); 
//            return ;
//        }
//        if ( a instanceof DeletePrefix ) {
//            DeletePrefix a2 = (DeletePrefix)a ;
//            target.deletePrefix(a2.gn, a2.prefix); 
//            return ;
//        }
        // Transaction.
        enact(a, target) ;
    }
    
    private void enact(ChangeItem a, RDFChanges target) {
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
    }
    
    public RDFChangesCollector() { }

    private void collect(ChangeItem object) { 
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
