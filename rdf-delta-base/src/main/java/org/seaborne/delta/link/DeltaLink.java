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

package org.seaborne.delta.link;

import java.util.List;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch ;

/** Interface to the server for the operations.
 * A server is any engine that that provides the Delta operations
 * and it may be local or remote.
 * <p> The client connection for operations on a specific dataset is {@code DeltaConnection}.
 */
public interface DeltaLink {
    /** 
     * Create a new dataset and return the unique Id for it.
     * <p>
     * The {@code name} is an unused name for this link; it is a file name (not a path).
     * <p>
     * The {@code uri} is carried around with the data source.
     */  
    public Id newDataSource(String name, String uri) ;
    
    /** Make a dataset unavailable.
     *  Actual deleting of resources depends on the implementation.
     *  (A server will tend not to really delete a persistent database.)
     */
    public void removeDataset(Id dsRef);

    /** Return an array of ids of datasets */
    public List<Id> listDatasets() ;
    
    /** Return details of a dataset (or null if not registered) */
    public DataSourceDescription getDataSourceDescription(Id dsRef) ;

    /** Send patch, return new version */
    public int sendPatch(Id dsRef, RDFPatch patch);
    
    public int getCurrentVersion(Id dsRef);

    /** Retrieve a patch by datasource and version, and write it to the {@code OutputStream}. */ 
    public RDFPatch fetch(Id dsRef, int version);

    /** Retrieve a patch and write it to the {@code OutputStream}. */ 
    public RDFPatch fetch(Id dsRef, Id patchId);

    /** Create an {@link RDFChanges} for this link. */
    public RDFChanges createRDFChanges(Id dsRef);
    
//    public void existingDataset() {} 
//    
//    public Id newDataset() { return null ; }
//    public void deleteDataset(Id uuid) { }
//
//    // Graph-only system
//    
//    public Id newGraph(String uri) { return null ; }

    /** Register a client id.
     * Only one client can be registered on a link at a time.
     * @param clientId
     * @return RegToken
     */
    public RegToken register(Id clientId);

//    public RegToken register(String name);
//    public RegToken register(String name, Id id);
    
    public void deregister();

    /** Check whether registered for this link. */
    public boolean isRegistered();

    /** Return the registration token, or null if not registered.
     * <p>
     * This operation is local. 
     */
    public RegToken getRegToken();
    
    /** Return the registration token, or null if not registered.
     * <p>
     * This operation is local. 
     */
    public Id getClientId();
}
