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

import java.util.Objects;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonNumber;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonValue;
import org.seaborne.delta.lib.JSONX;

/** A Version */
public class Version implements Comparable<Version> {
    // Certain well known versions.
    /** The version number when not set */
    public static Version UNSET = new Version(DeltaConst.VERSION_UNSET, "<unset>");
    /** The version when there are no patches */
    public static Version INIT  = new Version(DeltaConst.VERSION_INIT, "<init>");
    /** The version number of the first patch (default) */
    public static Version FIRST = Version.create(DeltaConst.VERSION_FIRST);

    private final long version;
    private final String display;

    public static Version fromJson(JsonObject obj, String field, Version dftValue) {
        long ver = JSONX.getLong(obj, field, dftValue.version) ;
        return create(ver);
    }

    public static Version fromJson(JsonObject obj, String field) {
        long ver = JSONX.getLong(obj, field, -99) ;
        if ( ver < -1 )
            throw new DeltaException("Bad version number: '"+JSON.toStringFlat(obj.get(field))+"'");
        return create(ver);
    }

    /** Display name for a version (as long) */
    public static String str(long version) {
        if ( version == DeltaConst.VERSION_UNSET )
            return "unset";
        if ( version == DeltaConst.VERSION_INIT )
            return "init";
        return Long.toString(version);
    }


    public static Version create(JsonValue version) {
        Objects.requireNonNull(version, "version");
        if ( version.isNumber() ) {
            long ver = JSONX.getLong(version, -2);
            if ( ver < -1 )
                throw new DeltaException("Bad version number: '"+JSON.toStringFlat(version)+"'");
            return create(ver);
        }

        if ( version.isString() ) {
            try {
                String s = version.getAsString().value();
                long ver = Long.parseLong(s);
                return create(ver);
            } catch (NumberFormatException ex) {
                throw new DeltaException("Bad format for version: '"+JSON.toStringFlat(version)+"'");
            }
        }
        throw new DeltaException("Unrecognized JSON version: '"+JSON.toStringFlat(version)+"'");
    }

    public static Version create(String string) {
        Objects.requireNonNull(string);
        long x = Long.parseLong(string);
        return Version.create(x);
    }

    public static Version create(long version) {
        // Versions count from 1 or use a constant,
        if ( version == UNSET.value() )
            return UNSET;
        if ( version == INIT.value() )
            return INIT;
        if ( version <= 0 )
            throw new DeltaException();
        return new Version(version, null);
    }

    private Version(long version, String display) {
        this.version = version;
        this.display = display;
    }

    public long value() {
        return version;
    }

    public Version inc() {
        if ( this == INIT )
            return FIRST;
        if ( ! isValid() )
            throw new DeltaException("Attempt to get inc version on a non-version number: "+this);
        return Version.create(version+1);
    }

    public Version dec() {
        if ( this == INIT || this == UNSET )
            throw new DeltaException("Attempt to get version before a non-version number: "+this);
        if ( ! isValid() )
            throw new DeltaException("Attempt to get dec version on a non-version number: "+this);
        return Version.create(version-1);
    }

    /** Is this version a possible version? (i.e. not a marker) */
    public static boolean isValid(Version version) {
        if ( version == null )
            return false;
        return version.isValid();
    }

    /** Is this version a possible version? (i.e. not a marker) */
    public static boolean isValid(long version) {
        return version > Version.INIT.version ;
    }

    /** Is this version a possible version? (i.e. not a marker) */
    public boolean isValid() {
        //return this != Version.UNSET && this != Version.INIT ;
        return version != Version.UNSET.value() && version != Version.INIT.value() ;
    }

    public boolean isUnset() {
        return version == Version.UNSET.value();
    }

    public JsonValue asJson() {
        return JsonNumber.value(version) ;
    }

    public String asParam() {
        return Long.toString(version) ;
    }

    // Does not use display for equality.

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int)(version ^ (version >>> 32));
        return result;
    }

    @Override
    public int compareTo(Version other) {
        Objects.requireNonNull(other);
        return Long.compare(this.version, other.version);
    }

    public boolean isBefore(Version other) {
        Objects.requireNonNull(other);
        int x = this.compareTo(other);
        return x < 0 ;
    }

    public boolean isAfter(Version other) {
        Objects.requireNonNull(other);
        int x = this.compareTo(other);
        return x > 0 ;
    }

    @Override
    public boolean equals(Object obj) {
        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        Version other = (Version)obj;
        if ( version != other.version )
            return false;
        return true;
    }

    @Override
    public String toString() {
        if ( display != null )
            return display;
        return "ver:"+Long.toString(version);
    }
}
