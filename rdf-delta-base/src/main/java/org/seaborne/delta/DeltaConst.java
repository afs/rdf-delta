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

import java.util.regex.Pattern ;

import org.apache.jena.atlas.web.AcceptList ;
import org.apache.jena.atlas.web.ContentType ;
import org.apache.jena.sparql.util.Symbol ;

public class DeltaConst {
    // Letters, numbers, "."  "_" and "-"
    // Can't start with a "-" or "."
    public static final String  DataSourceRegexStr = "^[\\w_\\$][\\w-\\._\\$]*$";
    public static final Pattern DataSourceRegex    = Pattern.compile(DataSourceRegexStr);

    // PatchStore implementations
    // Name of the file basename for file stored patches.
    public static final String FilePatchBasename = "patch";


    // Endpoints.
//    public static final String EP_PatchLog     = "patch-log";
//    public static final String EP_Fetch        = "fetch";
//    public static final String EP_Append       = "patch";

    public static final String EP_InitData     = "init-data";
    public static final String EP_Ping         = "$/ping";
    public static final String EP_RPC          = "$/rpc";

    // RPC calls - operation names.
    public static final String OP_PING           = "ping";
    public static final String OP_LIST_DS        = "list_datasource";
    public static final String OP_LIST_DSD       = "list_descriptions";
    public static final String OP_DESCR_DS       = "describe_datasource";
    public static final String OP_DESCR_LOG      = "describe_log";
    public static final String OP_LIST_LOG_INFO  = "list_log_info";
    public static final String OP_CREATE_DS      = "create_datasource";
    public static final String OP_REMOVE_DS      = "remove_datasource";

    // JSON field names, in RPC and configuration files.
    public static final String F_OP            = "operation";
    public static final String F_ARG           = "arg";
    public static final String F_DATASOURCE    = "datasource";
    public static final String F_STORAGE       = "storage";
    public static final String F_CLIENT        = "client";
    public static final String F_TOKEN         = "token";
    // For tracking.
    public static final String F_OP_ID         = "opid";

    // Used as an alternative to "name" - now consolidated on "name"
    // May need to reuse field at sometime.
    @Deprecated
    public static final String F_BASE          = "base";

    public static final String F_SOURCES       = "sources";
    public static final String F_ID            = "id";
    public static final String F_VERSION       = "version";
    public static final String F_LOCATION      = "location";
    public static final String F_MINVER        = "min_version";
    public static final String F_MAXVER        = "max_version";
    public static final String F_LATEST        = "latest";
    public static final String F_NAME          = "name";
    public static final String F_DATA          = "data";
    public static final String F_URI           = "uri";
    public static final String F_LOG_TYPE      = "log_type";
    // Some atomic JSON value.
    public static final String F_VALUE         = "value";
    // Some JSON array
    public static final String F_ARRAY         = "array";

    /** Default choice of port */
    public static final int    PORT            = 1066;
    public static final int    SYSTEM_VERSION  = 1;

    // Short names of log providers. Lowercase.
    public static final String LOG_FILE        = "file";
    public static final String LOG_MEM         = "mem";
    public static final String LOG_SQL         = "sql";
    public static final String LOG_S3          = "s3";

    // Properties used to define patch store providers.
    public static final String pDeltaFile      = "delta.file";
    public static final String pDeltaZk        = "delta.zk";

    // HTTP query string.
    // Registration
    public static final String paramRef        = "ref";
    public static final String paramClient     = F_CLIENT;
    public static final String paramToken      = F_TOKEN;

    public static final String paramPatch      = "patch";
    public static final String paramDatasource = F_DATASOURCE;
    public static final String paramVersion    = "version";

    // Symbols used to store information, e.g. in a dataset context.

    public static final String symBase              = "delta:"; //"http://jena.apache.org/delta#";
    public static final Symbol symDeltaClient       =  Symbol.create(symBase+"client");
    public static final Symbol symDeltaConnection   =  Symbol.create(symBase+"connection");
    public static final Symbol symDeltaZone         =  Symbol.create(symBase+"zone");

    // Content type constants for RDF Patch.
    public static final String contentTypePatchText     = "application/rdf-patch";
    public static final String contentTypePatchTextAlt  = "text/rdf-patch";
    public static final String contentTypePatchBinary   = "application/rdf-patch+thrift";

    // Preferred form.
    public static final ContentType ctPatchText         = ContentType.create(contentTypePatchText);
    public static final ContentType ctPatchBinary       = ContentType.create(contentTypePatchBinary);

    public static final AcceptList rsOfferPatch         = AcceptList.create(contentTypePatchText,
                                                                            contentTypePatchTextAlt,
                                                                            contentTypePatchBinary);

    // Environment variable name for the runtime area for the Delta server.
    public static final String ENV_BASE        = "DELTA_BASE";

    // Environment variable name for the installation area for the Delta server.
    public static final String ENV_HOME        = "DELTA_HOME";

    // Environment variable name for the port number of the Delta server.
    public static final String ENV_PORT        = "DELTA_PORT";

    /** The size of the server-wide LRU cache */
    public static final int PATCH_CACHE_SIZE   = 1000;

    /** The version number when not set */
    public static long VERSION_UNSET    = -1;

    /** The version number when there are no patches */
    public static long VERSION_INIT     = 0;

    /** The version number of the first patch (default) */
    public static long VERSION_FIRST    = 1;
}
