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

import org.seaborne.delta.Id;

/** Snapshot of the state of a {@link PatchLog}. */
public class PatchLogInfo {
    private final long minVersion ;
    private final long maxVersion ;
    private final Id dataSourceId ;
    private final Id latestPatch ;
    
    
    /*package*/PatchLogInfo(Id dsRef, long minVersion, long maxVersion, Id latestPatch) {
        this.minVersion = minVersion ;
        this.maxVersion = maxVersion ;
        this.dataSourceId = dsRef ;
        this.latestPatch = latestPatch ;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dataSourceId == null) ? 0 : dataSourceId.hashCode());
        result = prime * result + ((latestPatch == null) ? 0 : latestPatch.hashCode());
        result = prime * result + (int)(maxVersion ^ (maxVersion >>> 32));
        result = prime * result + (int)(minVersion ^ (minVersion >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        PatchLogInfo other = (PatchLogInfo)obj;
        if ( dataSourceId == null ) {
            if ( other.dataSourceId != null )
                return false;
        } else if ( !dataSourceId.equals(other.dataSourceId) )
            return false;
        if ( latestPatch == null ) {
            if ( other.latestPatch != null )
                return false;
        } else if ( !latestPatch.equals(other.latestPatch) )
            return false;
        if ( maxVersion != other.maxVersion )
            return false;
        if ( minVersion != other.minVersion )
            return false;
        return true;
    }
}