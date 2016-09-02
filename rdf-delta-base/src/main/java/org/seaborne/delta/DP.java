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

import java.util.concurrent.atomic.AtomicInteger ;

public class DP {
    
    // RPC calls.
    /** Name of the operation field */ 
    public static final String F_OP         = "operation" ;
    public static final String F_ARG        = "arg" ;    
    public static final String OP_EPOCH     = "epoch" ;
    
    /** Default choice of port */
    public static final int PORT = 1066 ;
    
    public static final String EP_RPC     = "rpc" ;
    public static final String EP_Patch   = "patch" ;
    public static final String EP_FETCH   = "fetch" ;
    
//    public static final String PatchContainer   = "http://localhost:"+PORT+"/patch" ;
//    
//    public static final String _FetchService     = "http://localhost:"+PORT+"/fetch" ;

    static AtomicInteger lastPatchFetch = new AtomicInteger(0) ;
}
