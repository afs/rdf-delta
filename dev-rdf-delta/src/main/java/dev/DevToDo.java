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

package dev;

public class DevToDo {
    // Initial data : "version 0"
    // getInitialData -> stream of quads (as a patch?)
    // XXX [INIT]
    // DLink : get URL for initial data. (file, 
    // DeltaConnection.initData : read URL. 
    //   Conneg for DPS.
    /*
    <mime-type>application/rdf+xml;charset=utf-8</mime-type>
    <mime-type>text/turtle;charset=utf-8</mime-type>
    <mime-type>text/plain;charset=utf-8</mime-type>
    <mime-type>text/nquads;charset=utf-8</mime-type>
    <mime-type>application/trig;charset=utf-8</mime-type>
    
    */
    // Migrate PatchLogServer

    // Log to update triple store
    // Log storage abstraction.
    
    // Start from blank each time (ignore "state") vs start from current state.

    // Renames:
    //    RDFChanges -> StreamPatch
    // 
    //   Send patch => append patch
    
    //   Server PatchLog -> PatchLogStore
    //   DeltaConnection -> PatchLog, DeltaPatchLog, DataSource
    //     PatchLogConnection PatchLogConn
    
    // Tests:
    //   bad patch
    //   empty patch
    
    // Documentation
    //   Patch
    //   Protocol/delta
    // Version -> long?
    // Eliminate use of Location and just use Path.
    
    // Streaming patches.
    // Currently not because of DeltaLink.sendPatch takes a patch as argument. 
    
    //PatchLog - conflates "index" and "version" - acceptable?
    //          - revisit HistoryEntry - it keeps patches? LRU cache of patches.  
    
    // ------------------------------------------------------------------------
    // Document, "Design" protocol
    // -- History and parenthood.

    // -- javacc tokenizer:
    //   prefix name in ^^xsd:foo. (what does TokenizerText do? SubToken)
    //   See TestTokenizerJavaccTerms
    //     "0xXYZ"
    //     "''@lang- "

    // -- Others
    // Extract polling support to DeltaClient.

    // DatasetGraphBuffering
}
