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

package txnx;

import static org.apache.jena.query.ReadWrite.WRITE ;

import java.util.concurrent.Semaphore ;
import java.util.concurrent.atomic.AtomicReference ;

import org.apache.jena.atlas.lib.Lib ;
import org.apache.jena.query.ReadWrite ;
import org.apache.jena.sparql.JenaTransactionException ;
import org.apache.jena.sparql.core.Transactional ;

/** A Transactional 'X' supporting multiple readers and a single writer. */ 
public abstract class TransactionalBlob<X> implements Transactional {
    
    // ---- TransactionCoordinator.
    
    // Semaphore to implement "Single Active Writer" - independent of readers
    // This is not reentrant.
    private Semaphore writersWaiting = new Semaphore(1, true) ;
    
    private void releaseWriterLock() {
        int x = writersWaiting.availablePermits() ;
        if ( x != 0 )
            throw new JenaTransactionException("TransactionCoordinator: Probably mismatch of enable/disableWriter calls") ;
        writersWaiting.release() ;
    }
    
    private boolean acquireWriterLock(boolean canBlock) {
        if ( ! canBlock )
            return writersWaiting.tryAcquire() ;
        try { 
            writersWaiting.acquire() ; 
            return true;
        } catch (InterruptedException e) { throw new JenaTransactionException(e) ; }
    }
    // ---- TransactionCoordinator.
    
    // Transaction state accessors.
    /** Provide a snapshot of the input.
     *  If 'object' is immutable, this can be just return the input.
     *  If it might be mutated later, then thismust be a copy.
     *  This code never mutates {@code <X>}.
     */
    protected abstract X snapshot(X object) ;
    
    // Global state - the exterally visible value and the starting point for any
    // transaction. This is set to a new value when a write trasnaction commits.
    private AtomicReference<X> value = new AtomicReference<>(null) ; 

    // ---- Transaction state.
    
    // The per-transaction state (inside a transaction). Null outside a transaction
    // cleared by commit or abort in a write transaction.
    private ThreadLocal<X> txnValue = ThreadLocal.withInitial(()->null) ;
    // The kind of transaction.
    private ThreadLocal<ReadWrite>    txnMode  = ThreadLocal.withInitial(()->null) ;
    
    // Syncrhonization for making changes.  
    private Object txnLifecycleLock   = new Object() ; 
    
    protected TransactionalBlob(X initial) {
        value.set(initial) ;
    }

    // Sometime its easier to delay intialization e.g. the subclass constructor needs to do some work first.  
    protected void setInitial(X initial) {
        value.set(initial) ;
    }
    
    @Override
    public void begin(ReadWrite readWrite) {
        begin(readWrite, true) ;
    }
    
    public void begin(ReadWrite readWrite, boolean canBlock) {
        // Ensure a single writer. 
        // (Readers never block at this point.)
        if ( readWrite == WRITE ) {
            // Writers take a WRITE permit from the semaphore to ensure there
            // is at most one active writer, else the attempt to start the
            // transaction blocks.
            // Released by in commit/abort.
            acquireWriterLock(canBlock) ;
        }
        // at this point, 
        // One writer or one of many readers. 
        
        synchronized(txnLifecycleLock) {
            if ( txnMode.get() != null )
                throw new JenaTransactionException("Already in a transaction") ;
            X state = snapshot(value.get()) ;
            txnValue.set(state) ;
            txnMode.set(readWrite);
        }
    }

    @Override
    public void commit() {
        checkTxn(); 
        if ( isWriteTxn() ) {
            commitPrepare() ;
            value.set(getDataState()) ;
            txnValue.set(null); 
            releaseWriterLock();
            commitFinish() ;
        }
        endOnce() ;
    }

    /** Prepare for commit in a write transaction - includes writing state to disk */ 
    protected void commitPrepare() {}
    /** Finish committing  in a write transaction */  
    protected void commitFinish() {}

    @Override
    public void abort() {
        checkTxn(); 
        if ( isWriteTxn() ) {
            txnValue.set(null); 
            releaseWriterLock();
        }
        endOnce() ;
    }

    @Override
    public boolean isInTransaction() {
        ReadWrite mode = txnMode.get() ;
        if ( mode == null )
            // Remove it - avoid holding the memory.
            txnMode.remove();
        return mode != null ;
    }

    @Override
    public void end() {
        if ( ! isInTransaction() ) 
            return ;
        if ( isWriteTxn() && txnValue.get() != null )
            throw new JenaTransactionException("No commit or abort before end for a write transaction") ;
        endOnce() ;
    }

    private void endOnce() {
        if ( isActiveTxn() ) {
            txnValue.remove();
            txnMode.remove();
        }
    }

    /** Set the value inside a write transaction, return the old value*/ 
    public X set(X x) {
        checkWriteTxn() ;
        X old = getDataState() ;
        setDataState(x);
        return old ;
    }
    

    /** Return the current value in a transaction. 
     * Must be inside a transaction. 
     * @see #getCurrent
     * @see #currentValue
     */
    public X get() {
        checkTxn();
        return getDataState() ;
    }

    /** Return the current value.
     * If inside a transaction, return the transaction view of the value.
     * If not in a transaction return the state value (effectively
     * a read transaction, optimized by the fact that reading the
     *  the state is atomic).
     */
    public X getCurrent() {
        if ( isActiveTxn() )
            return getDataState() ;
        else
            return value.get() ;
    }

    /**
     * Read the current global state (that is, the last committed value) outside a
     * transaction. This does not reflect the transaction if inside a transction this is
     * still the last committed value,
     */
    public X currentValue() {
        return value.get() ;
    }

    // These two operations do not clear the thread local if we are not in a transaction.
    // This is a potential memory leak.
    // Use "isInTransaction" to read and clear.

    /** Is this a write transaction? Should be called inside a transaction. */
    protected boolean isWriteTxn() {
        ReadWrite rw = txnMode.get() ;
        if ( rw == null )
            throw new JenaTransactionException(Lib.classShortName(this.getClass())+".isWriteTxn called outside a transaction") ;
        return txnMode.get() == ReadWrite.WRITE ;
    }

    protected boolean isActiveTxn() {
        ReadWrite rw = txnMode.get() ;
        return rw != null ;
    }

    /** Get the thread state */ 
    protected X getDataState() {
        return txnValue.get() ;
    }

    /** Set the thread state */ 
    protected void setDataState(X x) {
        txnValue.set(x) ;
    }

    protected void checkWriteTxn() {
        if ( ! isWriteTxn() )
            throw new JenaTransactionException("Not in a write transaction") ;
    }

    protected void checkTxn() {
        if ( ! isActiveTxn() )
            throw new JenaTransactionException("Not in a transaction") ;
    }
}
