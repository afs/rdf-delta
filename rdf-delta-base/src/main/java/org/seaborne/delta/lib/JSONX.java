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

package org.seaborne.delta.lib;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.function.Consumer ;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Additional JSON code */ 
public class JSONX {
    private static Logger LOG = LoggerFactory.getLogger("JsonAccess");
    
    private static String LABEL = "%%object%%" ;

    public static JsonObject buildObject(Consumer<JsonBuilder> setup) {
        JsonBuilder b = JsonBuilder.create().startObject(LABEL) ;
        setup.accept(b);
        return b.finishObject(LABEL).build().getAsObject() ;
    }

    /** Access a field of a JSON object : return as {@code Optional<String>} */ 
    public static Optional<String> getStr(JsonObject obj, String field) {
        return Optional.ofNullable(getStrOrNull(obj, field));
    }
    
    /** Access a field of a JSON object : return a string or null */ 
    public static String getStrOrNull(JsonObject obj, String field) {
        if ( obj == null )
            System.err.println("getStrOrNull: null object");
        
        JsonValue jv = obj.get(field);
        if ( jv == null )
            return null;
        if ( jv.isString() )
            return jv.getAsString().value();
        return null ;
    }
    
    /** Access a field of a JSON object : return a {@code long}, or the default value. */
    public static long getLong(JsonObject obj, String field, long dftValue) {
        JsonValue jv = obj.get(field);
        if ( jv == null )
            return dftValue;
        return getLong(jv, dftValue);
    }
    
    /** JSON value to long: return a {@code long}, or the default value. */
    public static long getLong(JsonValue jv, long dftValue) {
        if ( jv.isNumber() ) {
            Number num = jv.getAsNumber().value();
            if ( num.doubleValue() < Long.MIN_VALUE || num.doubleValue() > Long.MAX_VALUE )
                throw new NumberFormatException("Number out of range: "+jv);
            return num.longValue();
        }
        return dftValue ;
    }

    /** Access a field of a JSON object, return an {@code int} or a default value. */ 
    public static int getInt(JsonObject obj, String field, int dftValue) {
        JsonValue jv = obj.get(field);
        if ( jv == null )
            return dftValue;
        return getInt(jv, dftValue);
    }
    
    /** JSON value to int: return a {@code long}, or the default value. */
    public static int getInt(JsonValue jv, int dftValue) {
        if ( jv.isNumber() ) {
            long z = jv.getAsNumber().value().longValue();
            if ( z < Integer.MIN_VALUE || z > Integer.MAX_VALUE )
                throw new NumberFormatException("Number out of range: "+jv);
            return (int)z;
        }
        return dftValue ;
    }

    /** Create a safe copy of a {@link JsonValue}.
     * <p>
     *  If the JsonValue is a structure (object or array), copy the structure recursively.
     *  <p>
     *  If the JsonValue is a primitive (string, number, boolean or null),
     *  it is immutable so return the same object.  
     */
    public static JsonValue copy(JsonValue arg) {
        JsonBuilder builder = builder(arg) ;
        return builder==null ? arg : builder.build() ;
    }
    
    /** Create a builder from a {@link JsonValue}.
     *  <p>If the argument is an object or array, use it to initialize the builder.
     *  <p>If the argument is a JSON primitive (string, number, boolean or null),
     *  <p>Otherwise thrown {@link IllegalArgumentException}.
     */
    public static JsonBuilder builder(JsonValue arg) {
        if ( arg.isObject() ) {
            JsonObject obj = arg.getAsObject() ;
            JsonBuilder builder = JsonBuilder.create() ;
            builder.startObject() ;
            obj.forEach((k,v) -> builder.key(k).value(copy(v))) ;
            builder.finishObject() ;
            return builder ; 
        }
        if ( arg.isArray() ) {
            JsonArray array = arg.getAsArray() ;
            JsonBuilder builder = JsonBuilder.create() ;
            builder.startArray() ;
            array.forEach((a)->builder.value(copy(a))) ;
            builder.finishArray() ;
            return builder ; 
        }
        throw new IllegalArgumentException("Not a JSON object or JSON array; "+arg);
    }
    
    /** Return a {@link JsonObject} parsed out of bytes. */ 
    public static JsonObject fromBytes(byte[] bytes) {
        return JSON.parse(new ByteArrayInputStream(bytes));
    }

    public static byte[] asBytes(JsonValue jsonValue) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeFlat(out, jsonValue);
        return out.toByteArray();
    }
    
    /** Write out a JSON value - pass a JSON Object to get legal exchangeable JSON */
    private static void writeFlat(OutputStream output, JsonValue jValue) {
        IndentedWriter iOut = new IndentedWriter(output) ;
        iOut.setFlatMode(true);
        JSON.write(iOut, jValue) ;
        iOut.flush() ;
    }

}
