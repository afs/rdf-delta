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

import static org.seaborne.delta.DeltaConst.F_LATEST ;
import static org.seaborne.delta.DeltaConst.F_MAXVER ;
import static org.seaborne.delta.DeltaConst.F_MINVER ;

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
    private final DataSourceDescription dsd;
    private final long minVersion ;
    private final long maxVersion ;
    private final Id latestPatch ;
    
    public PatchLogInfo(DataSourceDescription dsd, long minVersion, long maxVersion, Id latestPatch) {
        this.dsd = dsd;
        this.minVersion = minVersion ;
        this.maxVersion = maxVersion ;
        this.latestPatch = latestPatch ;
    }

    /* This is a superset of DataSourceDescription
     * {
     *    id:
     *    name:
     *    uri:
     *    minVersion:
     *    maxVersion:
     *    latestPatch:
     * }
     */
    
    public JsonObject asJson() {
        return JSONX.buildObject(b->addJsonFields(b));
    }
    
    /** Insert as a nested-object into the builder */ 
    public void addJsonObject(JsonBuilder b) {
        b.startObject();
        addJsonFields(b);
        b.finishObject();
    }
    
    /** Add fileds to current JsonBuilder object */
    public void addJsonFields(JsonBuilder b) {
        dsd.addJsonFields(b);
        b.key(F_MINVER).value(minVersion);
        b.key(F_MAXVER).value(maxVersion);
        if ( latestPatch != null )
            b.key(F_LATEST).value(latestPatch.asString());
        else
            b.key(F_LATEST).value("");
    }
    
    public static PatchLogInfo fromJson(JsonObject obj) {
        DataSourceDescription dsd = DataSourceDescription.fromJson(obj);
        long minVer = JSONX.getLong(obj, F_MINVER, DeltaConst.VERSION_UNSET) ;
        long maxVer = JSONX.getLong(obj, F_MAXVER, DeltaConst.VERSION_UNSET) ;
        String latestPatchStr = JSONX.getStrOrNull(obj, F_LATEST);
        Id latestPatch = null;
        if ( latestPatchStr != null && !latestPatchStr.isEmpty() )
            latestPatch = Id.fromString(latestPatchStr);
        return new PatchLogInfo(dsd, minVer, maxVer, latestPatch); 
    }
    
    @Override
    public String toString() {
        return String.format("[%s, %s, [%s,%s] <%s>]",
                             getDataSourceId(), getDataSourceName(), 
                             verString(minVersion), verString(maxVersion),
                             (latestPatch==null)?"":latestPatch.toString());
    }

    private static String verString(long version) {
        if ( version == DeltaConst.VERSION_UNSET )
            return "--";
        return Long.toString(version);
    }
    
    public DataSourceDescription getDataSourceDescr() {
        return dsd;
    }


    public Id getDataSourceId() {
        return dsd.getId();
    }

    public String getDataSourceName() {
        return dsd.getName();
    }

    public String getDataSourceURI() {
        return dsd.getUri();
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dsd == null) ? 0 : dsd.hashCode());
        result = prime * result + ((latestPatch == null) ? 0 : latestPatch.hashCode());
        result = prime * result + (int)(maxVersion ^ (maxVersion >>> 32));
        result = prime * result + (int)(minVersion ^ (minVersion >>> 32));
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
        PatchLogInfo other = (PatchLogInfo)obj;
        if ( dsd == null ) {
            if ( other.dsd != null )
                return false;
        } else if ( !dsd.equals(other.dsd) )
            return false;
        if ( latestPatch == null ) {
            if ( other.latestPatch != null )
                return false;
        } else if ( !latestPatch.equals(other.latestPatch) )
            return false;
        if ( maxVersion != other.maxVersion )
            return false;
        if ( minVersion != other.minVersion )
            return false;
        return true;
    }

}
