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

import org.apache.jena.atlas.lib.Registry ;
import org.slf4j.Logger ;

/** The registry of all data under the control of server */ 
public class DataRegistry extends Registry<Id, DataSource> {
    
    private static Logger LOG = DPS.LOG ;
    private final String label ; 
    
    public DataRegistry(String label) {
        this.label = label ;
    }
    
    // Probably need separate registries to divide up the managed space.
    // e.g. dev-staging-prod
    
    private static DataRegistry singleton = new DataRegistry("central") ;

    public static DataRegistry get() { return singleton ; }
    
    @Override
    public void put(Id key, DataSource ds) {
        LOG.info("Register: "+key );
        super.put(key, ds) ;
    }
    
    @Override
    public DataSource get(Id key) {
        return super.get(key) ;
    }

    @Override
    public String toString() {
        return String.format("Registry: '%s': size=%d", label, super.size()) ; 
    }
}
