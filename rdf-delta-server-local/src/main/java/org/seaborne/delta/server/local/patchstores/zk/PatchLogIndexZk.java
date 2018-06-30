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

package org.seaborne.delta.server.local.patchstores.zk;

import java.util.List;
import java.util.NoSuchElementException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonValue;
import org.apache.jena.atlas.logging.Log;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.Id;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;

/** State control for a {@link PatchStore} */
public class PatchLogIndexZk implements PatchLogIndex {
    private final CuratorFramework client;
    private final String statePath;
    private final String versionsPath;
    
    private long earliestVersion = DeltaConst.VERSION_UNSET;
    private Id earliestId = null;
    
    private long version = DeltaConst.VERSION_UNSET;
    private Id current = null;
    private Id previous = null;
    // XXX Need watcher.
    
    private final String fVersion = "version";
    private final String fId = "id";
    private final String fPrevious = "previous";
    
    public PatchLogIndexZk(CuratorFramework client, String statePath, String versionsPath) {
        this.client = client ;
        Zk.zkEnsure(client, statePath);
        Zk.zkEnsure(client, versionsPath);
        this.statePath = statePath;
        this.versionsPath = versionsPath;
        
        JsonObject obj = Zk.zkFetchJson(client, statePath);
        if ( obj == null ) {
            save(DeltaConst.VERSION_INIT, null, null);
            earliestVersion = DeltaConst.VERSION_INIT;
            return;
        }
        
        version = obj.get(fVersion).getAsNumber().value().longValue();
        current = getIdOrNull(obj, fId);
        previous = getIdOrNull(obj, fPrevious);
        
        // Find earliest.
        List<String> x = Zk.zkSubNodes(client, versionsPath);
        //Guess: 1
        if ( x.isEmpty() )
            earliestVersion = DeltaConst.VERSION_INIT;
        else if ( x.contains("00000001") )
            // Fast-track the "obvious" answer
            earliestVersion = 1;
        else {
            try {
                earliestVersion = x.stream().map(this::versionFromName).filter(v->(v>0)).min(Long::compare).get();
            } catch (NoSuchElementException ex) {  }
        }
        earliestId = mapVersionToId(earliestVersion);
    }

    private Id getIdOrNull(JsonObject obj, String field) {
        JsonValue jv = obj.get(field);
        if ( jv == null )
            return null;
        String s = jv.getAsString().value();
        return Id.fromString(s);
    }

    @Override
    public boolean isEmpty() {
        //return version == DeltaConst.VERSION_UNSET || DeltaConst.VERSION_INIT;
        return current == null;
    }

    @Override
    public long nextVersion() {
        return version+1;
    }
    
    @Override
    public Id mapVersionToId(long ver) {
        if ( ver == DeltaConst.VERSION_INIT || ver == DeltaConst.VERSION_UNSET )
            return null;
        String p = versionPath(ver);
        byte[] b = Zk.zkFetch(client, versionPath(ver));
        if ( b == null )
            return null;
        Id id = Id.fromBytes(b);
        return id;
    }

    private String versionPath(long ver) { return Zk.zkPath(versionsPath, String.format("%08d", ver)); }
    
    private long versionFromName(String name) {
        try {
            return Long.parseLong(name);
        } catch (NumberFormatException ex) {
            Log.warn(this, "Attempt to extract the version from '"+name+"'");
            return -1;
        }
    }
    
    @Override
    public void save(long version, Id patch, Id prev) {
        this.version = version;
        this.current = patch;
        this.previous = prev;
        JsonObject x = state(version, patch, prev);
        if ( patch != null )
            Zk.zkCreateSet(client, versionPath(version), patch.asBytes());
        Zk.zkSetJson(client, statePath, x);
    }
    
    private JsonObject state(long version, Id patch, Id prev) {
        return JSONX.buildObject(b->{
            b.pair(fVersion, version);
            if ( patch != null )
                b.pair(fId, patch.asPlainString());
            if ( prev != null )
                b.pair(fPrevious, patch.asPlainString());
        }); 
    }
    
    @Override
    public long getCurrentVersion() {
        return version;
    }

    @Override
    public Id getCurrentId() {
        return current;
    }

    @Override
    public Id getPreviousId() {
        return previous;
    }

    @Override
    public long getEarliestVersion() {
        return earliestVersion;
    }

    @Override
    public Id getEarliestId() {
        return earliestId;
    }

}
