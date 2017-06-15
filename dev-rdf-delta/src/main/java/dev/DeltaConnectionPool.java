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

package dev;

import java.util.concurrent.ArrayBlockingQueue ;
import java.util.concurrent.BlockingQueue ;

import org.seaborne.delta.DeltaException ;
import org.seaborne.delta.Id ;
import org.seaborne.delta.client.DeltaConnection ;

public class DeltaConnectionPool {
    
    interface IDeltaConnection {
        
    }
    
    // Unbounded.
    private final BlockingQueue<DeltaConnectionPooled> pool = new ArrayBlockingQueue<>(100);

    // Add call back on close.
    
    DeltaConnectionPool(DeltaConnection ... connections) {
        for ( DeltaConnection dc: connections )
            pool.add(new DeltaConnectionPooled(this, dc));
    }
    
    public DeltaConnection get(Id datasourceId) {
        try {
            return pool.take();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    static class DeltaConnectionPooled extends DeltaConnection {

        private final DeltaConnectionPool pool ;

        protected DeltaConnectionPooled(DeltaConnectionPool pool, DeltaConnection other) {
            super(other) ;
            this.pool = pool;
        }

        @Override public void close() {
            super.close();
            pool.returnToPool(this);
        }
    }
    
    private void returnToPool(DeltaConnectionPooled dConn) {
        interrupable(()->pool.put(dConn));
    }
    
    private void addToPool(DeltaConnection dConn1) {
        DeltaConnectionPooled dConn2 = new DeltaConnectionPooled(this, dConn1);
        interrupable(()->pool.put(dConn2));
    }
    
    interface RunnableInterruptable { public void run() throws InterruptedException; }
    
    private void interrupable(RunnableInterruptable action) {
        try { action.run(); }
        catch (InterruptedException e) {
            e.printStackTrace();
            throw new DeltaException("InterruptedException", e); 
        }
    }
    
}
