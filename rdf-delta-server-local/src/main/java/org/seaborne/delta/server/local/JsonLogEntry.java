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

package org.seaborne.delta.server.local;

import org.apache.jena.atlas.json.JsonObject;
import org.seaborne.delta.Id;
import org.seaborne.delta.Version;
import org.seaborne.delta.lib.JSONX;

/** To and from the JSON representation of a LogEntry */
public class JsonLogEntry {

    private static final String fVersion  = "version";
    private static final String fId       = "id";
    private static final String fPrevious = "previous";

    /** Encode a log entry as a JSON object */
    public static JsonObject logEntryToJson(long version, Id patch, Id prev) {
        return JSONX.buildObject(b->{
            b.pair(fVersion, version);
            if ( patch != null )
                b.pair(fId, patch.asPlainString());
            if ( prev != null )
                b.pair(fPrevious, prev.asPlainString());
        });
    }

    /** Encode a log entry as a JSON object */
    public static JsonObject logEntryToJson(LogEntry entry) {
        return logEntryToJson(entry.getVersion().value(), entry.getPatchId(), entry.getPrevious());
    }

    /** Decode a JSON Object to obtain a log entry */
    public static LogEntry jsonToLogEntry(JsonObject obj) {
        Id patchId = getIdOrNull(obj, fId);
        Id prevId = getIdOrNull(obj, fPrevious);
        long ver = JSONX.getLong(obj, fVersion, -99);
        Version version = (ver == -99) ? Version.UNSET : Version.create(ver);
        return new LogEntry(patchId, version, prevId);
    }

    private static Id getIdOrNull(JsonObject obj, String field) {
        String s = JSONX.getStrOrNull(obj, field);
        if ( s == null )
            return null;
        return Id.fromString(s);
    }
}
