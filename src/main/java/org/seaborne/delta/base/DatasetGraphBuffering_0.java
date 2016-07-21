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

import org.apache.jena.query.ReadWrite ;
import org.apache.jena.sparql.JenaTransactionException ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.seaborne.delta.changes.StreamChanges ;
import org.seaborne.delta.changes.StreamChangesApply ;
import org.seaborne.delta.changes.StreamChangesBuffering ;

/** A {@link DatasetGraph} implementation that buffers adds/deletes and prefix changes
 * then plays them to another {@link DatasetGraph} or to a {@link StreamChanges}. 
 * <p>
 * This has the capacity to add {@link DatasetGraph#abort} functionality to a {@link DatasetGraph} 
 * that does not provide it. 
 */
public class DatasetGraphBuffering_0 extends DatasetGraphChanges {
    /*
     * An alternative is record the effect of all quads and triples added but check
     * whether an add undoes a previous delete, or whether a delete removes a buffered
     * triple. Then it does not matter if adds or deletes are applied first.
     * 
     * Variant: do all the work on deletes (less common?) and just keep all adds; play
     * adds then deletes.
     * 
     * Variant: check whether add or delete will have any effect by checking the
     * underlying DatasetGraph. This saves some space at the expense of complixity and
     * also needing the underlying DatasetGraph to be available and cheap to do single
     * adds and deletes. The recorded change has an associate object wrapper and repeated
     * adds or deletes need not be recorded.
     * 
     * This StreamChanges verison records the actions API performed so it can be played
     * againts different datasets, or sent to another place. The StreamChanges can form a
     * persistent record of changes. It also a lot simpler as it builds on the
     * StreamChanges classes.
     * 
     * It does not require the underlying DatasetGraph to be accessed on each add or
     * delete (c.f. variant that checks changes) and can capture the changes using one of
     * the StreamChanges classes.
     */

    public DatasetGraphBuffering_0(DatasetGraph dsg) { 
        super(dsg, new StreamChangesBuffering()) ;
    }
    
    public void play(DatasetGraph other) {
        StreamChangesBuffering scBuffer = buffered() ;
        StreamChanges dest = new StreamChangesApply(other) ; 
        scBuffer.play(dest);
    }
    
    public void play(StreamChanges other) {
        StreamChangesBuffering scBuffer = buffered() ;
        scBuffer.play(other); 
    }
    
    private StreamChangesBuffering buffered() {
        return (StreamChangesBuffering)(super.monitor) ;
    }
    
    // To get consistency/locking right, we open a transaction at the start.
    @Override
    public void begin(ReadWrite readWrite) {
        // If the underlying database does not support transactions,
        // or is in a transaction already, this will cause an exception. 
        get().begin(readWrite) ;
    }

    @Override
    public void commit() {
        checkIsInTransaction() ;
        play(get()) ;
        get().commit() ; 
        buffered().reset() ;
    }

    @Override
    public void abort() {
        checkIsInTransaction() ;
        if ( get().supportsTransactionAbort() )
            get().abort() ;
        else
            // Don't replay - just commit no changes.
            get().commit() ;
        buffered().reset() ;
    }

    @Override
    public void end() {
        get().end() ;
    }

    @Override
    public boolean supportsTransactions() {
        return true ;
    }
    
    @Override
    public boolean supportsTransactionAbort() {
        return true ;
    }

    private void checkIsInTransaction() {
        if ( ! get().isInTransaction() )
            throw new JenaTransactionException("Not in a transaction") ;
    }
}
