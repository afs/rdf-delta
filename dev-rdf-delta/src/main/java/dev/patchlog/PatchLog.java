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

package dev.patchlog;

import org.seaborne.delta.Id ;
import org.seaborne.delta.PatchLogInfo ;
import org.seaborne.patch.RDFPatch ;

public interface PatchLog {
    
    /** Send patch, return new version */
    public long append(String dsName, RDFPatch patch);
    
    /** Get the current version: if this is an HTTP connection, this causes network traffic. */
    public default long getCurrentVersion(String dsName) { return getPatchLogInfoByName(dsName).getMaxVersion(); }

    /** Retrieve a patch by datasource and version. */ 
    public RDFPatch fetch(String dsName, long version);

    /** Retrieve a patch by datasource and patch id. */ 
    public RDFPatch fetch(String dsName, Id patchId);
    
    /** Return details of the patch log (or null if not registered) */
    public PatchLogInfo getPatchLogInfoByName(String dsName) ;
}
