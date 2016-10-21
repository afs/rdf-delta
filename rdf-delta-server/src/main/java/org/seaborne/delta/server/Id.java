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

import java.util.UUID ;

import org.apache.jena.shared.uuid.UUIDFactory ;
import org.apache.jena.shared.uuid.UUID_V1_Gen ;

/**
 * 
 */
public final class Id {
    public static Id create() {
        return new Id(genUUID()) ;
    }

    public static Id fromUUID(UUID uuid) { return new Id(uuid) ; } 
    
    public static Id fromString(String str) {
        try {
            UUID uuid = UUID.fromString(str) ;
            return new Id(uuid) ; 
        } catch (IllegalArgumentException ex) {
            return new Id(str) ;
        }
    }

    // Fix version as version 1 - these are guessable.
    private static UUIDFactory uuidFactory = new UUID_V1_Gen() ;

    /** {@link UUID}s are used to UUIDentify many things in Delta - the RDF Dataset being managed,
     * the patches applied (the UUID naming forms the history), registrations and channels,
     * amongst other things.
     */
    public static UUID genUUID() { return uuidFactory.generate().asUUID() ; }

    private final UUID uuid ;
    private final String string ;

    private Id(UUID id) {
        uuid = id ;
        string = null ;
    }

    private Id(String id) {
        uuid = null ;
        string = id ;
    }

    @Override
    public String toString() {
        if ( uuid != null ) 
            return uuid.toString() ;
        return string ;
    }

    @Override
    public int hashCode() {
        final int prime = 31 ;
        int result = 1 ;
        result = prime * result + ((string == null) ? 0 : string.hashCode()) ;
        result = prime * result + ((uuid == null) ? 0 : uuid.hashCode()) ;
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
        Id other = (Id)obj ;
        if ( string == null ) {
            if ( other.string != null )
                return false ;
        } else if ( !string.equals(other.string) )
            return false ;
        if ( uuid == null ) {
            if ( other.uuid != null )
                return false ;
        } else if ( !uuid.equals(other.uuid) )
            return false ;
        return true ;
    } 
}
