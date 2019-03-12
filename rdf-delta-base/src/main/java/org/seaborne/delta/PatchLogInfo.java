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

package org.seaborne.delta;

import static org.seaborne.delta.DeltaConst.F_LATEST ;
import static org.seaborne.delta.DeltaConst.F_MAXVER ;
import static org.seaborne.delta.DeltaConst.F_MINVER ;
import static org.seaborne.delta.DeltaOps.*;

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
    private final Version minVersion ;
    private final Version maxVersion ;
    private final Id latestPatch ;
    
    public PatchLogInfo(DataSourceDescription dsd, Version minVersion, Version maxVersion, Id latestPatch) {
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
    
    /** Add fields to current JsonBuilder object */
    public void addJsonFields(JsonBuilder b) {
        dsd.addJsonFields(b);
        b.key(F_MINVER).value(minVersion.asJson());
        b.key(F_MAXVER).value(maxVersion.asJson());
        if ( latestPatch != null )
            b.key(F_LATEST).value(latestPatch.asString());
        else
            b.key(F_LATEST).value("");
    }
    
    public static PatchLogInfo fromJson(JsonObject obj) {
        DataSourceDescription dsd = DataSourceDescription.fromJson(obj);
        Version minVer = Version.fromJson(obj, F_MINVER, Version.UNSET) ;
        Version maxVer = Version.fromJson(obj, F_MAXVER, Version.UNSET) ;
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

    public Version getMinVersion() {
        return minVersion ;
    }

    public Version getMaxVersion() {
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
        result = prime * result + ((maxVersion == null) ? 0 : maxVersion.hashCode());
        result = prime * result + ((minVersion == null) ? 0 : minVersion.hashCode());
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
        if ( maxVersion == null ) {
            if ( other.maxVersion != null )
                return false;
        } else if ( !maxVersion.equals(other.maxVersion) )
            return false;
        if ( minVersion == null ) {
            if ( other.minVersion != null )
                return false;
        } else if ( !minVersion.equals(other.minVersion) )
            return false;
        return true;
    }
}
