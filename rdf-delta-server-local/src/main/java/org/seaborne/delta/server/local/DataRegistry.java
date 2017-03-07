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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.atlas.lib.Registry ;
import org.seaborne.delta.Id;
import org.slf4j.Logger ;

/** The registry of all data under the control of server */ 
public class DataRegistry extends Registry<Id, DataSource> {
    
    private static Logger LOG = DPS.LOG ;
    private final String label ; 
    // Index DataSources by URI, only if the URI is not null. 
    private Map<String, DataSource> indexByURI = new ConcurrentHashMap<>();   
    
    public DataRegistry(String label) {
        this.label = label ;
    }
    
    @Override
    public void put(Id key, DataSource ds) {
        LOG.info("Register datasource: "+key );
        super.put(key, ds) ;
        if ( ds.getURI() != null )
            indexByURI.put(ds.getURI(), ds);
    }
    
    @Override
    public DataSource get(Id key) {
        return super.get(key) ;
    }

    public DataSource get(String uri) {
        return indexByURI.get(uri);
    }

    @Override
    public String toString() {
        if ( label != null )
            return String.format("Registry: '%s': size=%d : %s", label, super.size(), super.keys()) ;
        else
            return String.format("Registry: size=%d : %s", super.size(), super.keys()) ; 
    }
}
