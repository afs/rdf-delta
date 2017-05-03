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

import java.util.UUID;

import org.seaborne.delta.Id ;

/** registration */
public class RegToken {
    private final UUID uuid;

    private static String label = "token:";
    
    public RegToken() {
        this.uuid = UUID.randomUUID();
    }

    public RegToken(String uuid) {
        if ( uuid.startsWith(label))
            uuid = uuid.substring(label.length());
        this.uuid = UUID.fromString(uuid);
    }

    public UUID getUUID() {
        return uuid;
    }

    // Suitable for use in RPC.
    public String asString() {
        return "token:"+uuid;
    }

    // For Display 
    @Override
    public String toString() {
        return "token:"+Id.shortUUIDstr(uuid);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
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
        RegToken other = (RegToken)obj;
        if ( uuid == null ) {
            if ( other.uuid != null )
                return false;
        } else if ( !uuid.equals(other.uuid) )
            return false;
        return true;
    }
}