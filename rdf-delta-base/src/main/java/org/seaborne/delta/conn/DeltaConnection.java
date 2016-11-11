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

package org.seaborne.delta.conn;

import org.seaborne.patch.RDFPatch ;

/** Connection from client to server.
 * Can deal with many DataSources per connection.    
 */
public interface DeltaConnection {
//    public RegToken register(Id clientId);
//    public RegToken register(String name);
//    public RegToken register(String name, Id id);
//    
//    public void deregister(RegToken token);

    /** Send patch */
    public void sendPatch(Id dsRef, RDFPatch patch);
    
    public int getCurrentVersion(Id dsRef);

    /** Retrieve a patch by datasource and version, and write it to the {@code OutputStream}. */ 
    public RDFPatch fetch(Id dsRef, int version);

    /** Retrieve a patch and write it to the {@code OutputStream}. */ 
    public RDFPatch fetch(Id dsRef, Id patchId);
    
//    public void existingDataset() {} 
//    
//    public Id newDataset() { return null ; }
//    public void deleteDataset(Id uuid) { }
//
//    // Graph-only system
//    
//    public Id newGraph(String uri) { return null ; }

}
