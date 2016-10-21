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

package org.seaborne.delta.server;

/** Snapshot of the state of a {@link PatchSet}. */
public class PatchSetInfo {
    private final long minVersion ;
    private final long maxVersion ;
    private final Id patchSetId ;
    private final Id latest ;
    
    
    /*package*/PatchSetInfo(long minVersion, long maxVersion, Id target, Id latest) {
        this.minVersion = minVersion ;
        this.maxVersion = maxVersion ;
        this.patchSetId = target ;
        this.latest = latest ;
    }

    @Override
    public int hashCode() {
        final int prime = 31 ;
        int result = 1 ;
        result = prime * result + (int)(minVersion ^ (minVersion >>> 32)) ;
        result = prime * result + (int)(maxVersion ^ (maxVersion >>> 32)) ;
        result = prime * result + ((patchSetId == null) ? 0 : patchSetId.hashCode()) ;
        result = prime * result + ((latest == null) ? 0 : latest.hashCode()) ;
        return result ;
    }

    @Override
    public boolean equals(Object obj) {
        if ( this == obj )
            return true ;
        if ( obj == null )
            return false ;
        if ( getClass() != obj.getClass() )
            return false ;
        PatchSetInfo other = (PatchSetInfo)obj ;

        if ( maxVersion != other.maxVersion )
            return false ;
        if ( minVersion != other.minVersion )
            return false ;
        
        if ( latest == null ) {
            if ( other.latest != null )
                return false ;
        } else if ( !latest.equals(other.latest) )
            return false ;
        
        if ( patchSetId == null ) {
            if ( other.patchSetId != null )
                return false ;
        } else if ( !patchSetId.equals(other.patchSetId) )
            return false ;
        
        return true ;
    }
    
}