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

package org.seaborne.patch.system;

import java.util.Iterator ;
import java.util.function.Consumer ;

import org.apache.jena.graph.Graph ;
import org.apache.jena.graph.Node ;
import org.apache.jena.query.ReadWrite ;
import org.apache.jena.query.TxnType ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.DatasetGraphWrapper ;
import org.apache.jena.sparql.core.Quad ;
import org.seaborne.patch.RDFChanges ;

/**
 * Connect a {@link DatasetGraph} with {@link RDFChanges}. All operations on the
 * {@link DatasetGraph} that cause changes have the change sent to the
 * {@link RDFChanges}.
 * <p>
 * Optionally, a sync handler can be given that is called on {@code sync()} or {@code begin}.  
 * This class is stateless so updating the wrapped dataset is possible via the sync handler.  
 * <p>
 * Synchrionization can also be performed externally on the wrapped dataset.  
 * <p>
 * Use {@link DatasetGraphRealChanges} to get a dataset that logs only changes that have a
 * real effect - that makes the changes log reversible (play delete for each add) to undo
 * a sequence of changes.
 * 
 * @see DatasetGraphRealChanges
 * @see RDFChanges
 */
public class DatasetGraphChanges extends DatasetGraphWrapper {
    // Break up?
    // inherits DatasetGraphRealChanges < DatasetGraphAddDelete
    
    protected final Runnable syncHandler;
    protected final Consumer<ReadWrite> txnSyncHandler;
    protected final RDFChanges monitor;
    private static Runnable identityRunnable = ()->{};
    private static <X> Consumer<X> identityConsumer() { return (x)->{}; }

    /** Create a {@code DatasetGraphChanges} which does not have any sync handlers */
    public DatasetGraphChanges(DatasetGraph dsg, RDFChanges monitor) {
        this(dsg, monitor, identityRunnable, identityConsumer());
    }
    
    /** Create a {@code DatasetGraphChanges} which calls different patch log synchronization handlers on {@link #sync} and {@link #begin}.
     *  {@code syncHandler} defaults (with null) to "no action".
     *  
     *  Transactional usage preferred. 
     */
    public DatasetGraphChanges(DatasetGraph dsg, RDFChanges monitor, Runnable syncHandler, Consumer<ReadWrite> txnSyncHandler) {
        super(dsg) ; 
        this.monitor = monitor ;
        this.syncHandler = syncHandler == null ? identityRunnable : syncHandler;
        this.txnSyncHandler = txnSyncHandler == null ? identityConsumer() : txnSyncHandler;
    }
    
    public RDFChanges getMonitor() { return monitor; }
    
    @Override public void sync() {
        syncHandler.run();
        if ( syncHandler != identityRunnable )
            super.sync();
    }
    
    @Override
    public void add(Quad quad) {
        add(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject());
    }
    
    @Override
    public void delete(Quad quad) {
        delete(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject());
    }
    
    @Override
    public void add(Node g, Node s, Node p, Node o) {
        monitor.add(g, s, p, o);
        super.add(g, s, p, o) ;
    }
    
    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        monitor.delete(g, s, p, o);
        super.delete(g, s, p, o) ;
    }
    
    @Override
    public Graph getDefaultGraph()
    { return new GraphChanges(get().getDefaultGraph(), null, monitor) ; }

    @Override
    public Graph getGraph(Node graphNode)
    { return new GraphChanges(get().getGraph(graphNode), graphNode, monitor) ; }
    
    private static final int DeleteBufferSize = 10000 ;
    @Override
    /** Simple implementation but done without assuming iterator.remove() */
    public void deleteAny(Node g, Node s, Node p, Node o) {
        Quad[] buffer = new Quad[DeleteBufferSize];
        while (true) {
            Iterator<Quad> iter = find(g, s, p, o);
            // Get a slice
            int len = 0;
            for ( ; len < DeleteBufferSize ; len++ ) {
                if ( !iter.hasNext() )
                    break;
                buffer[len] = iter.next();
            }
            // Delete them.
            for ( int i = 0 ; i < len ; i++ ) {
                delete(buffer[i]);
                buffer[i] = null;
            }
            // Finished?
            if ( len < DeleteBufferSize )
                break;
        }
    }
    
    private boolean isWriteMode() {
        return super.transactionMode() == ReadWrite.WRITE ; 
    }


    // In case an implementation has one "begin" calling another.
    // XXX Per thread?  Or a general lock?
    private boolean insideBegin = false ;

    @Override
    public void begin() {
        if ( insideBegin ) {
            super.begin(); 
            return;
        }
        insideBegin = true;
        try {
            // For the sync, we have to assume it will write.
            // Any potential write causes a write-sync to be done in "begin".
            txnSyncHandler.accept(ReadWrite.WRITE);
            if ( transactionMode() == ReadWrite.WRITE )
                monitor.txnBegin();
        } finally {
            insideBegin = false;
        }
    }
    
    @Override
    public void begin(TxnType txnType) {
        if ( insideBegin ) {
            super.begin(txnType); 
            return ;
        }
            
        insideBegin = true;
        try {
            // For the sync, we have to assume it will write.
            ReadWrite readWrite = ( txnType == TxnType.READ) ? ReadWrite.READ : ReadWrite.WRITE; 
            txnSyncHandler.accept(readWrite);
            super.begin(txnType);
            if ( transactionMode() == ReadWrite.WRITE )
                monitor.txnBegin();
        } finally {
            insideBegin = false;
        }
    }
        
    @Override
    public void begin(ReadWrite readWrite) {
        if ( insideBegin ) {
            super.begin(readWrite); 
            return ;
        }
        insideBegin = true;
        try {
            txnSyncHandler.accept(readWrite);
            super.begin(readWrite);
            if ( transactionMode() == ReadWrite.WRITE )
                monitor.txnBegin();
        } finally {
            insideBegin = false;
        } 
    }
    
    @Override
    public boolean promote() {
        // Any potential write causes a write-sync to be done in "begin".
        // Here we are inside the transaction so calling the sync handler is not possible (nestd transaction risk). 
        if ( super.transactionMode() == ReadWrite.READ ) {
            boolean b = super.promote();
            if ( super.transactionMode() == ReadWrite.WRITE ) {
//                // Promotion.
//                if ( transactionType() == TxnType.READ_COMMITTED_PROMOTE )
//                    txnSyncHandler.accept(ReadWrite.WRITE);
                // We have gone ReadWrite.READ -> ReadWrite.WRITE
                monitor.txnBegin();
            }
            return b;
        }
        //Already WRITE.
        return super.promote();
    }

    @Override
    public void commit() {
        // Assume commit will work - signal first.
        if ( isWriteMode() )
            monitor.txnCommit();
        super.commit();
    }
    
    @Override
    public void abort() {
        // Assume abort will work - signal first.
        if ( isWriteMode() )
            monitor.txnAbort();
        super.abort();
    }

//    @Override
//    public void end() {
//        super.end();
//    }
    
}
