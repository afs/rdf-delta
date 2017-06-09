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

import static org.seaborne.delta.DeltaConst.F_ID ;
import static org.seaborne.delta.DeltaConst.F_LATEST ;
import static org.seaborne.delta.DeltaConst.F_MAXVER ;
import static org.seaborne.delta.DeltaConst.F_MINVER ;
import static org.seaborne.delta.DeltaConst.F_NAME ;

import org.apache.jena.atlas.json.JsonBuilder ;
import org.apache.jena.atlas.json.JsonObject ;
import org.seaborne.delta.lib.JSONX ;

/** 
 * Snapshot of the state of a {@code PatchLog}. 
 * This description of the patch log is "at a point in time" and is fixes.
 * It goes out of date as the patch log evolves.
 * 
 * @see DataSourceDescription
 */
public class PatchLogInfo {
    private final Id dataSourceId ;
    private final String dataSourceName ;
    private final long minVersion ;
    private final long maxVersion ;
    private final Id latestPatch ;
    
    public PatchLogInfo(Id dsRef, String name, long minVersion, long maxVersion, Id latestPatch) {
        this.dataSourceId = dsRef ;
        this.dataSourceName = name ;
        this.minVersion = minVersion ;
        this.maxVersion = maxVersion ;
        this.latestPatch = latestPatch ;
    }
    
    /*
     * {
     *    id:
     *    name:
     *    minVersion:
     *    maxVersion:
     *    latestPatch:
     * }
     */
    
    public JsonObject asJson() {
        return JSONX.buildObject(b->asJson(b));
    }
    
    /** Insert fields */ 
    public void asJson(JsonBuilder b) {
        b.key(F_ID).value(dataSourceId.asString());
        b.key(F_NAME).value(dataSourceName);
        b.key(F_MINVER).value(minVersion);
        b.key(F_MAXVER).value(maxVersion);
        if ( latestPatch != null )
            b.key(F_LATEST).value(latestPatch.asString());
        else
            b.key(F_LATEST).value("");
    }
    
    /** Insert as an Object nested-object) into the builder */ 
    public void asJsonObject(JsonBuilder b) {
        b.startObject();
        asJsonFields(b);
        b.finishObject();
    }

    public void asJsonFields(JsonBuilder b) {
        b.key(F_ID).value(dataSourceId.asString());
        b.key(F_NAME).value(dataSourceName);
        b.key(F_MINVER).value(minVersion);
        b.key(F_MAXVER).value(maxVersion);
        if ( latestPatch != null )
            b.key(F_LATEST).value(latestPatch.asString());
        else
            b.key(F_LATEST).value("");
    }
    
    public static PatchLogInfo fromJson(JsonObject obj) {
        String dsRefStr = JSONX.getStrOrNull(obj, F_ID) ;
        String name = JSONX.getStrOrNull(obj, F_NAME) ;
        long minVer = JSONX.getLong(obj, F_MINVER, -1) ;
        long maxVer = JSONX.getLong(obj, F_MAXVER, -1) ;
        String latestPatchStr = JSONX.getStrOrNull(obj, F_LATEST);
        Id latestPatch = null;
        if ( latestPatchStr != null )
            latestPatch = Id.fromString(latestPatchStr);
        return new PatchLogInfo(Id.fromString(dsRefStr), name, minVer, maxVer, latestPatch); 
    }
    
    @Override
    public String toString() {
        return String.format("[%s, %s, [%d,%d] <%s>]",
                             dataSourceId, dataSourceName, 
                             minVersion, maxVersion,
                             (latestPatch==null)?"":latestPatch.toString());
    }

    public Id getDataSourceId() {
        return dataSourceId ;
    }

    public String getDataSourceName() {
        return dataSourceName ;
    }

    public long getMinVersion() {
        return minVersion ;
    }

    public long getMaxVersion() {
        return maxVersion ;
    }

    public Id getLatestPatch() {
        return latestPatch ;
    }
}
