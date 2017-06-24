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

import java.util.regex.Pattern ;

public class DeltaConst {
    // Letters, numbers, "."  "_" and "-"
    // Can't start with a "-".
    public static final String  DataSourceRegexStr = "^[\\w_\\.\\$][\\w-\\._\\$]*$";
    public static final Pattern DataSourceRegex    = Pattern.compile(DataSourceRegexStr);
    
    // Endpoints.
    public static final String EP_RPC          = "rpc";
    public static final String EP_PatchLog     = "patch-log";
    public static final String EP_Fetch        = "fetch";
    public static final String EP_Append       = "patch";
    public static final String EP_InitData     = "init-data";

    // RPC calls - operation names.
    public static final String OP_PING         = "ping";
    public static final String OP_REGISTER     = "register";
    public static final String OP_DEREGISTER   = "deregister";
    public static final String OP_ISREGISTERED = "isregistered";
    
    public static final String OP_LIST_DS      = "list_datasource";
    public static final String OP_LIST_DSD     = "list_descriptions";
    public static final String OP_DESCR_DS     = "describe_datasource";
    public static final String OP_DESCR_LOG    = "describe_log";
    public static final String OP_CREATE_DS    = "create_datasource";
    public static final String OP_REMOVE_DS    = "remove_datasource";

    // JSON field names, in RPC and configuration files.
    public static final String F_OP            = "operation";
    public static final String F_ARG           = "arg";
    public static final String F_DATASOURCE    = "datasource";
    public static final String F_CLIENT        = "client";
    public static final String F_TOKEN         = "token";
    public static final String F_BASE          = "base";
    public static final String F_PORT          = "port";
    public static final String F_SOURCES       = "sources";
    public static final String F_ID            = "id";
    public static final String F_VERSION       = "version";
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

    // HTTP query string.
    public static final String paramZone       = "zone";
    public static final String paramClient     = F_CLIENT;
    public static final String paramReg        = F_TOKEN;

    public static final String paramPatch      = "patch";
    public static final String paramDatasource = F_DATASOURCE;
    public static final String paramVersion    = "version";

    // Registration
    public static final String paramRef        = "ref";

    // Environment variable name for the runtime area for the Delta server.
    public static final String ENV_BASE        = "DELTA_BASE";

    // Environment variable name for the installation area for the Delta server.
    public static final String ENV_HOME        = "DELTA_HOME";

    // Environment variable name for the port number of the Delta server.
    public static final String ENV_PORT        = "DELTA_PORT";

    // Environment variable name for the configuration file.
    public static final String ENV_CONFIG      = "DELTA_CONFIG";

    // Default name for the server configuration file.
    public static final String SERVER_CONFIG   = "delta.cfg";
    
    // Name for the datasource configuration file.
    public static final String DS_CONFIG       = "source.cfg";

    // Relative path name in a DataSource for the "sources" area.
    public static final String SOURCES         = "Sources";
    
    // Relative path name in a DataSource for the log area.
    public static final String LOG             = "Log";
    
    // The intial data area - server side  
    //public static final String INITIAL_DATA    = "Data";
    public static final String INITIAL_DATA    = "data.ttl";

    // The database area - client side directory (TDB or with files)  
    public static final String DATA            = "data";
    
    /** Name of the file holding the persistent state, client DeltaConnection. */
    public static final String STATE_CLIENT    = "state";
    
    /** Name of the file holding the persistent state, local server data source .*/
    public static final String STATE_DS        = "state";
    
    /** Marker file for "deletes" data sources (they are only hidden) */  
    public static final String DISABLED        = "disabled";
    
    /** The size of the server-wide LRU cache */
    public static final int PATCH_CACHE_SIZE   = 1000;
    
    /** The version number when not set */
    public static long VERSION_UNSET    = -1;
    
    /** The version number when there are no patches */
    public static long VERSION_INIT     = 0;
    
    /** The version number of the first patch */
    public static long VERSION_FIRST    = 1;
}
