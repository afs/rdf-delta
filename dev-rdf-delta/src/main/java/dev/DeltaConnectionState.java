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

package dev;

import java.nio.file.Path;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.Id;
import org.seaborne.delta.PersistentState;
import org.seaborne.delta.RefLong;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.lib.IOX;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Track one remote datasource */  
public class DeltaConnectionState {
    
    private static Logger LOG = LoggerFactory.getLogger(DeltaConnectionState.class);
    private static String VERSION_FILE = "version";
    
    private final Id dsRef;
    private final DeltaConnection dConn;
    private Location state;
    private RefLong version;

    // DeltaConnection.
    // Part of ?
    // Add to?
    
    // Need a manager to stop two uses of the same state.
    // Via DeltaConnection
    
    public DeltaConnectionState(DeltaConnection dConn, Id dsRef, Location workspace) {
        this.dConn = dConn;
        this.dsRef = dsRef;
        // Memory and disk versions.
        this.state = workspace;
        // Load version
        loadState(workspace);
        int ver = (int)version.getInteger();
        FmtLog.info(LOG, "%s : version = %s", dConn.getName(), ver);
        dConn.setLocalVersionNumber((int)version.getInteger());
        //initData();
    }
    
    private void initData() {
        sync();
    }

    private void sync() {
        dConn.getRemoteVersionLatest();
        dConn.sync();
    }

    private void loadState(Location workspace) {
        if ( workspace.isMem() ) {
            loadStateEphemeral(workspace);
            return ;
        }
        // Persistent.
        // data?
        loadStatePersistent(workspace);
        return ;
        
    }
    
    private void loadStateEphemeral(Location workspace) {
        version = new RefLongMem(0);
    }

    private void loadStatePersistent(Location workspace) {
        Path p = IOX.asPath(workspace);
        Path versionFile = p.resolve(VERSION_FILE);
        version = new PersistentState(versionFile);
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
