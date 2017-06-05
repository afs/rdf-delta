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

public class DevDeltaToDo {

    // Cmds 
    // rm --server URL x1 x2 x3 ...
    // mk --server URL x1 x2 x3 ...
    // list -server=
    // server from environment variable? from conf file?
    
    // cmd: "resync" - rsync the local server from disk. Ho, ho, ho. 

    // No registration is "error", "warning".
    
    // Initial data.
    
    // DeltaLinkHTTP ToDo s about network handling
    //How does writing to disk work? Unify with collect->write
    
    // Interface for patch send, receive, get all.
    // PatchLog--
        
    // Id.nil for "no previous".
    // Id for "any previous" (risky!)
    
    // DeltaConnection pooling.
    // so try(DeltaConnection){} works well.
    
    // Zone to track remote server.
    //   ==> PatchLogServer
    //   DataSources, their state, adds and deletes.
    //   Manage a backing dataset. Zone 1:1 DLink.
    
    // Check bnode URIs
    
    // XXX remove DConn, DLink remote version getting, and only have PatchLogInfo.
    // Combine with Zone?
    // Get allLogStates at once.
    // zone.attach -- bad name.
    
    // Tests:
    //   bad patch
    //   empty patch
    
    // Tests for cmds.
    //   Get previous then append for dcmd
    
    // Write patch to tmp file, move into place to accept.
    //    Or verify in mem space then write when accepted.
    //    Current bug,
    
    // Check DeltaConnection : commit order

    // ** Initial data : "version 0"
    // getInitialData -> stream of quads (as a patch?)

    // Migrate PatchLogServer; relationship to Zone?
    // Migrate AutoCommitGraph; PrefixMappingTxn; PrefixMappingWrapper
    // Migrate?? TransactionHandlerDP_txn
    
    // URI design. RDF Patch REST 
    //   http://server:1066/{WebAppCxt}/{shortName}/
    //                                 /{shortName}/init = "version 0" but dataset vs patch.
    //                                 /{shortName}/current = "version MaxInt"
    //                                 /{shortName}/patch/version
    //                                 /{shortName}/patch/id
    // Container: POST = append; GET = description in JSON.
    //  Subunits are individual patches. 
    // Then delta is admin/control.

    // Log to update triple store
    // ** Log storage abstraction.

    // POST patch -> 201 Created + ** Location: **
    
    // Renames:
    //   Server PatchLog -> PatchLogStore
    //   DeltaConnection -> PatchLog, DeltaPatchLog, DataSource
    //     PatchLogConnection PatchLogConn
    
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
    // GraphBuffering
    // BufferingTupleSet<X>
    
}
