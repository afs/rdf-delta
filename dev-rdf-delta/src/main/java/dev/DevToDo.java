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
    // DeltaConnection.sync.
    //   local version = -1 ==> must initialize data. 
    
    // Tests:
    //   bad patch
    //   empty patch
    
    // Remove setBase ... or make graph relative. 
    
    // Documentation
    //   Patch
    //   Protocol/delta
    // Version -> long?
    // Eliminate use of Location and just use Path.
    
//    public class GraphChanges 
//    //extends GraphWrapper  -- Not a GraphWithPerform
//    extends WrappedGraph
    
    // Module? : delta-rpc ; hanlder design
    //    client and servlet
    // System-of-record :
    //    * ServiceLoader and add to PatchHandler chain.
    //    * need displatch by log name or id.
    
    // SOR
    //   DatasetGraph
    //   Stream of patches.

    /*
    Log - Append
    Log<X>
      append => start(token), write, finish(token)
    EventLog, PatchLog
  send = grab token then append => early feedback
  ** Archive log **
  Need a datastructure to record the log structure
  (version, id) log
  State: Keep start.
     */
    
    interface SOR {
        
    }
    
    
    // One endpoint variant: /patch-log
    // two endpotjb variant /patch, /fetch (= /patch/*) 
    // Sort out send/fetch endpoints.
    // Just one fetch type.
    
    // Streaming patches.
    //    RDFChanges -> StreamPatch
    // Currently not because of DeltaLink.sendPatch takes a patch as argument. 
    
    // Test cleaning:
    //   target/test/server/... target/Zone ... inconsistent placement. 
    
    // Server local "Patch" - relevant?
    //   PatchLog.HistoryEntry - keep jyst meta data and have a "RDFPatch cache".
    
    //PatchLog - conflates "index" and "version" - acceptable?
    //          - revisit HistoryEntry - it keeps patches? LRU cache of patches.  
    // 
    
    // PatchLog.VALIDATE_PATCH_LOG
    // Cache patches, not hold in memory.
    
    // rdf-delta -> Delta
    
    // ** Examples - can be used for testing.
    
    // * Missing is communicating about graphs that have been created and have been deleted and need to appear/disappear in every app server. There are probably other operations that I will find are needed.
    // * Clear-up items: tests for some error cases like out of sequence patches.
    // * Jena assembler integration
    // * Documentation
    // * An unknown is keeping a TBC/DP graph up to date as there is no natural point to check. Probably only on open in TBS, rather than it changing as the user edits it.
    // * EVN/EDG - creating new graphs, removing graphs.
    // * EVN/EDG - How new graph "appear" magically when created via one server and have to appear on all other app servers.

    // StandardOpenOption.CREATE_NEW -- atomic
    
    // ToDo
    // * License header
    // * Examples.
    // * Test apply patch, not in sequence
    
    // * Tests assembler
    // Assembler to have a DeltaClient.
    
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
