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

public class DPNames {
    
    // RPC calls - operationn ames.
    public static final String OP_EPOCH     = "epoch";
    public static final String OP_REGISTER  = "register";
    public static final String OP_LIST_DS   = "list_datasource";
    public static final String OP_DESCR_DS  = "describe_datasource";
    
    // RPC arg/result fields */
    public static final String F_OP         = "operation";
    public static final String F_ARG        = "arg";
    public static final String F_DATASOURCE = "datasource";
    public static final String F_CLIENT     = "client";
    public static final String F_TOKEN      = "token";

    /** Default choice of port */
    public static final int    PORT         = 1066;

    public static final String EP_RPC       = "rpc";
    public static final String EP_Patch     = "patch";
    public static final String EP_Fetch     = "fetch";

    public static final String paramZone    = "zone";
    public static final String paramPatch   = "patch";
    public static final String paramDataset = "dataset";
    public static final String paramVersion = "version";
    
    // JSON object member names.
    public static final String F_BASE       = "base";
    public static final String F_PORT       = "port";
    public static final String F_SOURCES    = "sources";
    public static final String F_ID         = "id";
    public static final String F_VERSION    = "version";
    public static final String F_URI        = "uri";
    public static final String F_RESULT     = "result";

    // Registration
    public static final String paramRef     = "ref";

    // Environment variable name for the runtime area for the Delta server.
    public static final String ENV_BASE     = "DELTA_BASE";

    // Environment variable name for the installation area for the Delta server.
    public static final String ENV_HOME     = "DELTA_HOME";

    // Environment variable name for the port number of the Delta server.
    public static final String ENV_PORT   = "DELTA_PORT";

    // Environment variable name for the configuration file.
    public static final String ENV_CONFIG  = "DELTA_CONFIG";

    // Default name for the server configuration file.
    public static final String SERVER_CONFIG      = "delta.cfg";
    // Name for the datasource configuration file.
    public static final String DATA_CONFIG        = "source.cfg";

    
    // Relative path name in ENV_BASE for the "sources" area.
    public static final String SOURCES      = "Sources";
    // Relative path name in ENV_BASE for the patches area.
    public static final String PATCHES      = "Patches"; 
    public static final String DATA         = "data";

    
}
