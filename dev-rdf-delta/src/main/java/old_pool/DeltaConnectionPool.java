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

package old_pool;

import java.util.concurrent.ConcurrentHashMap ;
import java.util.function.Function ;

import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.DatasetGraphFactory ;
import org.seaborne.delta.Id ;
import org.seaborne.delta.client.DeltaConnection ;
import org.seaborne.delta.client.Zone ;
import org.seaborne.delta.link.DeltaLink ;

// UNFINISHED

/** Pool
 * <pre>
 * DeltaConnectionPool dcPool = dLink.getDeltaConnectionPool();
 * try ( DeltaConnection dConn =  dcPool.getFromPool(datasourceId) ) {
 *     dConn.getDataset
 * }
 * </pre>
 * or
 * <pre>
 * DeltaConnectionPool dcPool = dLink.getDeltaConnectionPool();
 * try ( DatasetGraphConnected dsg = dcPool.getDataset(datasourceId) ) {
 *     dsg...
 * }
 * </pre>
 */
public class DeltaConnectionPool {
    
    private ConcurrentHashMap<Id/*data source*/, DeltaConnection> pool = new ConcurrentHashMap<>();
    private final Zone zone ;
    private final DeltaLink dLink ;
    private Function<Id, DeltaConnection> computeConnection ;
    
    // Generator of datasets / Map of existing ones.
    // Function<Id, DeltaConnection>
    // zone?
    public DeltaConnectionPool(Zone zone, DeltaLink dLink) {
        this.zone = zone;
        this.dLink = dLink;
        // Function to connect if needed.
        this.computeConnection = 
        //Function<Id, DeltaConnection> computer =
            (id) -> {
                DatasetGraph dsgBase = allocateDatasetGraph();
                return DeltaConnection.connect(zone, id, dsgBase, dLink);  
            };
        // **** Sketching
        
        // General but verbose.
        try ( DeltaConnection dConn =  this.getDeltaConnection(null) ;
             ) {
            DatasetGraph dsg = dConn.getDatasetGraph(); 
        }
        
        // Yukky?
        try ( DatasetGraphConnected dsg = this.getDataset(null) ) {
            
        }
    }

    private DatasetGraphConnected getDataset(Id datasourceId) {
        return null ;
    }

    private DeltaConnection getDeltaConnection(Id datasourceId) {
        return null ;
    }

    private DeltaConnection create(Id datasourceId) {
        return DeltaConnection.connect(zone, datasourceId, null, dLink);
    }

    // Make "DeltaConnection.close" collaborate. 
    DeltaConnection get(Id datasourceId) {
        // Dataset?
        return pool.computeIfAbsent(datasourceId, computeConnection);
    }

    private DatasetGraph allocateDatasetGraph() {
        //return DatasetGraphFactory.createTxnMem();
        return DatasetGraphFactory.createTxnMem();
    }

    public void release(DeltaConnection dConn) {
        pool.remove(dConn.getDataSourceId());
    }
    
}
