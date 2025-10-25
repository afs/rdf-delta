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

package org.seaborne.delta.server;

import org.seaborne.delta.DeltaException;

public enum Provider {
    UNSET, MEM, FILE, ROCKS, LOCAL;

    public static Provider create(String str) {
        if ( UNSET.name().equalsIgnoreCase(str) )   return UNSET;
        if ( MEM.name().equalsIgnoreCase(str) )     return MEM;
        if ( FILE.name().equalsIgnoreCase(str) )    return FILE;
        if ( ROCKS.name().equalsIgnoreCase(str) )   return ROCKS;
        if ( "rdb".equalsIgnoreCase(str) )          return ROCKS;
        if ( LOCAL.name().equalsIgnoreCase(str) )   return LOCAL;
        throw new DeltaException("Provider name '"+str+"'not recognized");
    }
}