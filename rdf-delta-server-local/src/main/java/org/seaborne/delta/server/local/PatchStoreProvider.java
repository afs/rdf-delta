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

import org.seaborne.delta.server.local.patchlog.PatchLog;
import org.seaborne.delta.server.local.patchlog.PatchStore;
import org.seaborne.delta.server.local.patchlog.PatchStoreMgr;

/** The provider (factory) of {@link PatchStore} implementations.
 * These are added to {@link PatchStoreMgr}.
 * There will be only one object of each {@code PatchStoreProvider}.  
 */
public interface PatchStoreProvider {

    /** Create the {@link PatchStore} object for this process.
     * This should boot itself to be able to report existing {@link PatchLog PatchLogs}. 
     */
    public PatchStore create() ;

    //public PatchStore delete() ;
    
    
}
