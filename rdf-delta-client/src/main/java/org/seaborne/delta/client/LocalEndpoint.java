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

package org.seaborne.delta.client;

import java.util.List;

import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.Id;
import org.seaborne.delta.RefLong;
import org.seaborne.delta.link.DeltaLink;

/** Track one remote datasource */  
public class LocalEndpoint {
    
    private final Id dsRef;
    private final DeltaLink dLink;
    private Location state;
    private RefLong version;

    // DeltaConnection.
    // Part of ?
    // Add to?
    
    public LocalEndpoint(DeltaLink deltaLink, Id dsRef, Location workspace) {
        this.dLink = deltaLink;
        this.dsRef = dsRef;
        // Memory and disk versions.
        this.state = workspace;
        // Load version
        loadState(workspace);
        initData();
    }
    
    private void initData() {
        sync();
    }

    private void sync() {
        long current = dLink.getCurrentVersion(dsRef);
    }

    private void loadState(Location workspace) {
        if ( workspace.isMem() ) {
            version = new RefLongMem(0);
        }
        
        

        return ;
        
    }

    public long getVersion() {
        return version.getInteger();
    }

    // get DSG
    // sync()
    
    // init
    
    // push new
    
    static class RefLongMem implements RefLong {
        private long value;

        RefLongMem(long x) { this.value = x; }

        @Override
        public long getInteger() {
            return value;
        }

        @Override
        public void setInteger(long value) { this.value = value; } 

        @Override
        public long inc() {
            return (++value);
        }
    }
}
