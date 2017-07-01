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

    //++++
    // Check for any implementation then...
    //    RDFChangesWriteUpdate [fixed] but not RDFChangesWriter
    // Remove RDFChanges.start/finish.
    //   (Or Patch.play to call start, finish?)
    //++++
    
    // PatchCache.
    // TestPatchStore(file).
    
    // DeltaClient.nameToId : have the zone know the name->id mapping.
    // Check resetDeltaClient in AbstractTestDeltaConnection
    
    // Ugly : AbstractTestDeltaConnection.change_read_new
//    if ( ! dClient.getZone().exists(dsRef) )
//        dClient.attach(dsRef, LocalStorageType.MEM);
//    else
//        dClient.connect(dsRef);

    
    // Deltaclient.nameToId - needs caching.
    // zone.create -> zone.setUp, register.
    
    // Zone restart. -> "connect"
    // Do we need DeltaClient.connect.managed?
    
    // Soem sort of "connectOrCreate" in DeltaClient.
    
    // PatchLog  concurrency.
    // PatchLog  Id for log separate from dsRef 

    // Run - add a delete case.
    
    // HTTP interface / Control interface :: clearer split.
    
    // DeltaConnection XXX's
    
    // Initial data in Dlink.createDataSources.
    
    // PatchLogServer
	//   Server lifecycle.
	// 
    //      ping (RPC1), RPC, HTTP / POST / GET / Non-retriable POST.
    //   Retry policy.
    //   op - ping - ping - op 

    // Registration token on S_Fetch
	// --autoregister
    
    // Protect read in S_Data.
    
    // Cmds 
    // rm --server URL x1 x2 x3 ...
    // mk --server URL x1 x2 x3 ...
    // list -server=
    // resync
    // append (gets patch head and fills that in)

    // Quick poll : server epoch - avoid calc of list of datasources
    // "ping" like (RPC1). Use ping.
    
    // ping to return timeofday.
    
    // "Get all patches x to y."
    
    // DeltaLinkHTTP ToDo about network handling
    // How does writing to disk work? Unify with collect->write
    
    // Interface for patch send, receive, get all.
    // PatchLog--
        
    // Id.nil for "no previous".
    // Id for "any previous" (risky!)

    // POST patch -> 201 Created + ** Location: **
    
    // DeltaConnection pooling.
    // so try(DeltaConnection){} works well.
    
    // Migrate PatchLogServer; relationship to Zone?
    
    // URI design. RDF Patch REST 
    //   http://server:1066/{WebAppCxt}/{shortName}/
    //                                 /{shortName}/init = "version 0" but dataset vs patch.
    //                                 /{shortName}/current = "version MaxInt"
    //                                 /{shortName}/patch/version
    //                                 /{shortName}/patch/id
    // Container: POST = append; GET = description in JSON.
    //  Subunits are individual patches. 
    // Then delta is admin/control.

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
