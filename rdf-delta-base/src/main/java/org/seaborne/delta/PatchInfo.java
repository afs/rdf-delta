/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta;

import java.util.Objects;

/** 
 * Information about an entry in a patch log.
 * 
 * @see DataSourceDescription
 * @see PatchLogInfo
 */
public class PatchInfo {
    private final Version version ;
    private final Id patch ;
    private final Id previous ;
    
    public PatchInfo(Id patch, Version version, Id previous) {
        Objects.requireNonNull(patch, "Patch");
        this.patch = patch;
        this.version = version;
        this.previous = previous;
    }

    public Version getVersion() {
        return version;
    }

    public Id getPatch() {
        return patch;
    }

    public Id getPrevious() {
        return previous;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((patch == null) ? 0 : patch.hashCode());
        result = prime * result + ((previous == null) ? 0 : previous.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
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
        PatchInfo other = (PatchInfo)obj;
        if ( patch == null ) {
            if ( other.patch != null )
                return false;
        } else if ( !patch.equals(other.patch) )
            return false;
        if ( previous == null ) {
            if ( other.previous != null )
                return false;
        } else if ( !previous.equals(other.previous) )
            return false;
        if ( version == null ) {
            if ( other.version != null )
                return false;
        } else if ( !version.equals(other.version) )
            return false;
        return true;
    }


}
