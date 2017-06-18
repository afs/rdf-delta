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

import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.DatasetGraphWrapper ;
import org.seaborne.delta.client.DeltaConnection ;

public class DatasetGraphConnected extends DatasetGraphWrapper implements AutoCloseable {

    private final DeltaConnectionPool pool ;
    private final DeltaConnection dConn ;

    public DatasetGraphConnected(int x, DatasetGraph dsg, DeltaConnection dConn, DeltaConnectionPool pool) {
        super(dsg) ;
        this.dConn = dConn;
        this.pool = pool; 
    }

    @Override
    public void close() {
        pool.release(dConn);
    }
}
