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

package org.seaborne.delta;

import org.apache.jena.atlas.json.JsonNumber;
import org.apache.jena.atlas.json.JsonValue;

/** A Version */
public class Version {
    // Certain well known versions.
    public static Version UNSET = new Version(DeltaConst.VERSION_UNSET, "<unset>");
    public static Version INIT = new Version(DeltaConst.VERSION_INIT, "<init>");
    
    private final long version;
    private final String display;

    public Version create(long version) { return new Version(version, null); }  
    
    private Version(long version, String display) {
        this.version = version;
        this.display = display;
    }
    
    public JsonValue asJson() {
        return JsonNumber.value(version) ;
    }
    
    // Does not use display for equality.
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int)(version ^ (version >>> 32));
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
        Version other = (Version)obj;
        if ( version != other.version )
            return false;
        return true;
    }

    @Override
    public String toString() {
        if ( display != null )
            return display;
        return "ver:"+Long.toString(version); 
    }
}
