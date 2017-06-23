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

    // FileStore/PatchLog - cachingheaders.
    // DeltLinkLocal - pipeline from Receiver.
    // Log Sink iterface
    
    // Id into rdf-patch.
    
    /*
     * State machine. Concurrent safe
Check all concurrent paths.
Plugin architecture for patch handlers
Service loader
Initial data

Use cases
Two triple stores, sync or batch
Embedded store
     */
    
    // * DeltaLink.getConnection
    // * Interface to read-write patches for SoR.
    // * Discard failed patches.
    // * HTTP interface / Control interface
    
    // DeltaConnection XXX's
    
    // PatchLog.HistoryEntry - why?
    //   If cache - put in filestore?
    
    // PatchLogServer
	//   Server lifecycle.
	// 
    //      ping (RPC1), RPC, HTTP / POST / GET / Non-retriable POST.
    //   Retry policy.
    //   op - ping - ping - op 
    
    // DeltaConnection + DSG = ?
    
    // DeltaLink
    // Remove: dLink.listDatasets() , only listDescriptions -> listDescriptions()
    
    // ** DeltaConnection clean-up.
    // Split create into create-> id, no connection. Adds to pool.
    // Always pool.
    // DLink..connect then only DeltaConnection.connect
    
    // DeltaConnection.connect
    // DeltaConnection.attach = connect + new state. = connect + dConn.setupLocal(dsg) -> ??
    // Take out registration.
    
    
    // Simplify DeltaConnection to be 

	// Registration token on S_Fetch
	// --autoregister
    
    // Tests for spaces in base name.
    // Initial data fetch.
    
    // Protect read in S_Data.
    
    // base is a directory with spaces in the path name.
    
    // Cmds 
    // rm --server URL x1 x2 x3 ...
    // mk --server URL x1 x2 x3 ...
    // list -server=
    // resync
    // append (gets patch head and fils that in)

	// Patch input and system of record API
	// Clearer HTTPand admin/RPC split.
	
    // Quick poll : server epoch - avoid calc of list of datasources
    // "ping" like (RPC1).
    
    // ping to return timeofday.
    
    // Concurrency while creating and deleting DataSources, accessing PatchLogs.
    
    // "Get all patches x to y."

    // Initial data in DeltaConnection.create.
    
    // Zone-only managed dataset.
    // Don't allow DSG in DeltaConnection.connect
    
    // **** DeltaConnectionPool.
    
    // DeltaPatchLog:
//        void append(RDFPatch patch, int version) {
//            // [DP-Fix]
//            // If the patch is bad, we need to remove it else it will be assimilated on restart.
//            // Timing hole.
    
    // DeltaLinkHTTP ToDo about network handling
    // How does writing to disk work? Unify with collect->write
    
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
    
    // Write patch to tmp file, move into place to accept.
    //    Or verify in mem space then write when accepted.
    //    Current bug,
    
    // Migrate PatchLogServer; relationship to Zone?
    // Migrate autocommit package.
    
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

    // -- javacc tokenizer:
    //   prefix name in ^^xsd:foo. (what does TokenizerText do? SubToken)
    //   See TestTokenizerJavaccTerms
    //     "0xXYZ"
    //     "''@lang- "

    // DatasetGraphBuffering
    // GraphBuffering
    // BufferingTupleSet<X>
}
