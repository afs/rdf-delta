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

import static org.seaborne.delta.DeltaConst.F_ID;
import static org.seaborne.delta.DeltaConst.F_NAME;
import static org.seaborne.delta.DeltaConst.F_BASE;
import static org.seaborne.delta.DeltaConst.F_URI;

import java.util.Objects;

import org.apache.jena.atlas.json.JsonBuilder;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.logging.Log;
import org.seaborne.delta.lib.JSONX;

/** The fixed information about a {@code DataSource}.
 *
 * @see PatchLogInfo
 */
public class DataSourceDescription {
    private final Id id;
    private final String uri;
    private final String name;

    public DataSourceDescription(Id id, String name, String uri) {
        super();
        this.id = Objects.requireNonNull(id);
        this.name = Objects.requireNonNull(name);
        this.uri = uri;
    }

    /*
     * {
     *    id:
     *    name:
     *    uri:
     * }
     */

    public JsonObject asJson() {
        return JSONX.buildObject(b->addJsonFields(b));
    }

    public void addJsonObject(JsonBuilder b) {
        b.startObject();
        addJsonFields(b);
        b.finishObject();
    }

    public void addJsonFields(JsonBuilder b) {
        b.key(F_ID).value(id.asString());
        b.key(F_NAME).value(name);
        if ( uri != null )
            b.key(F_URI).value(uri);
    }

    public static DataSourceDescription fromJson(JsonObject obj) {
        String idStr = JSONX.getStrOrNull(obj, F_ID);
        if ( idStr == null )
            throw new DeltaException("Missing \"id:\" in DataSourceDescription JSON");

        String name = JSONX.getStrOrNull(obj, F_NAME);
        if ( name == null ) {
            @SuppressWarnings("deprecation")
            String n = JSONX.getStrOrNull(obj, F_BASE);
            // Compatibility.
            Log.warn(DataSourceDescription.class, "Deprecated: Use of field name \"base\" - change to \"name\"");
            name = n;
        }
        if ( name == null )
            throw new DeltaException("Missing \"name:\" in DataSourceDescription JSON");

        String uri = JSONX.getStrOrNull(obj, F_URI);
        return new DataSourceDescription(Id.fromString(idStr), name, uri);
    }

    @Override
    public String toString() {
        return String.format("[%s, %s, <%s>]", id, name, uri);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((uri == null) ? 0 : uri.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
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
        DataSourceDescription other = (DataSourceDescription)obj;
        if ( id == null ) {
            if ( other.id != null )
                return false;
        } else if ( !id.equals(other.id) )
            return false;
        if ( uri == null ) {
            if ( other.uri != null )
                return false;
        } else if ( !uri.equals(other.uri) )
            return false;
        return true;
    }

    public Id getId() {
        return id ;
    }

    public String getUri() {
        return uri ;
    }

    public String getName() {
        return name ;
    }
}
