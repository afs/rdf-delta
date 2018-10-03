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

package org.seaborne.delta.lib;

import org.apache.jena.util.Metadata;

public class SystemInfo {
    /**
     * A relative resources path to the location of
     * <code>fuseki-properties.xml</code> file.
     */
    static private String   metadataLocation             = "org/seaborne/delta/delta-properties.xml";

    /**
     * Object which holds metadata specified within
     * {@link Fuseki#metadataLocation}
     */
    static private Metadata metadata                     = initMetadata();

    private static Metadata initMetadata() {
        Metadata m = new Metadata();
        m.addMetadata(metadataLocation);
        return m;
    }

    /** Property path name base */
    private static final String PATH = "org.seaborne.delta";

    public static String systemName() { return "RDF Delta"; }
    public static String version() { return metadata.get(PATH + ".version", "development"); }
    public static String buildDate() { return metadata.get(PATH + ".build.datetime", "unknown"); }

//    /** The system name.*/
//    static public final String        NAME              = "RDF Delta";
//
//    /** Version of this code */
//    static public final String        VERSION           = metadata.get(PATH + ".version", "development");
//
//    /** Date when last built */
//    static public final String        BUILD_DATE        = metadata.get(PATH + ".build.datetime", "unknown");



}
