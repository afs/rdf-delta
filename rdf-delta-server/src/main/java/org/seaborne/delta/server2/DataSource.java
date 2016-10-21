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

package org.seaborne.delta.server2;

import org.apache.jena.tdb.base.file.Location ;

/** 
 * An item (one dataset and it's associated system resources)
 * under control of the server.
 * 
 * These are manged throught he {@link DataRegistry}.
 */
public class DataSource {
    
    // relationship to the Distributor?
    // Maybe one Distributor per DataSource (manages event flow). 
    
    private final Id id ;
    // Directory of all resources connected to this DataSourtce.
    private final Location location ;
    
    // Has version stuff
    private final PatchSet patchSet ;
    
    public DataSource(Id id, Location location, PatchSet patchSet) {
        super() ;
        this.id = id ;
        this.location = location ;
        this.patchSet = patchSet ;
        // read soiurce area.
        // check id (if not null)
        
    }

    public Id getId() {
        return id ;
    }

    public Location getLocation() {
        return location ;
    }

    public PatchSet getPatchSet() {
        return patchSet ;
    }

    
}
