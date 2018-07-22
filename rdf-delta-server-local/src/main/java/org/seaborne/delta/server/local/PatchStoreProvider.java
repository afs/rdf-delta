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

package org.seaborne.delta.server.local;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.seaborne.delta.server.local.patchstores.PatchStorage;

/** The provider (factory) of {@link PatchStore} implementations.
 * These are added to {@link PatchStoreMgr}.
 * There will be only one object of each {@code PatchStoreProvider}.  
 */
public interface PatchStoreProvider {

    /** 
     * Create the {@link PatchStore} object for this process.
     * Return null to signal the implementation is not available. 
     * This should boot itself to be able to report existing {@link PatchLog PatchLogs}. 
     */
    public PatchStore create(LocalServerConfig config);
    
    /** Long name */ 
    public String getProviderName();
    
    /** Short name used in server configuration files to set the default provider via "log_type" */ 
    public String getShortName();

    // These provide the policies for the choice of PatchLogIndex and PatchStorage.
    
    /** Create a new {@link PatchLogIndex} for the given {@link DataSourceDescription}. */
    public PatchLogIndex newPatchLogIndex(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration);
    
    /** Create a new {@link PatchStorage} for the given {@link DataSourceDescription}. */
    public PatchStorage newPatchStorage(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration);
}
