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
    // TxnSyncHandler and better DatasetGraphChanges for sync. 
    
    /* Examples: [DONE] */
    /* Document:
     * 
    Delta - home page.
       Use cases.
    Publishing RDF Changes - Synchronizing RDF Datasets
    RDF Patch - Defined the patch format.
    RDF Change Logs - defines the log protocol
        API, DeltaConenction.
        Different RDFChanges.
     */
    /* Use cases.
     * 
     *   Record changes for later processing, as an incremental backup, 
     *   Publish changes so others can keep their copy up to date, or choose which updates to apply. 
     *   Keep copies of a dataset in step.
     *   High Availability for SPARQL servers - transmit changes due to an update to all servers. 
     */
    
    // Add delta to SPARQL Update.
    // ** RDFChangesWriteUpdate
    
    // Check site
    //   Check issue #
    // Announce.
    
    // Merge registration of internal and external dsgs.
    // Assembler - where?
    // All tested!
    // Destination "file:" or local patch store?
    
    // ----
    // ** DatasetGraphChanges masks TDB ... DatasetGraph.exec(Op) ??
    // No - its a DSG wrapper -> is unwrapped.  
    
    // ** "Get all patches x to y."

    // PatchStoreMgr.selectPatchStore - need to map dsId to PatchStore or PatchLog. 
    
    // F_BASE vs F_NAME

    // Test RDFChangesCancelOnNoChange
    // RDFChanges.cancel :: TestRDFChangesMiscImpls
    
    // Logging : detect by presence of logging.properties etc.
    // Need logging package?
    
    // Tests for "non-rows" RDFPatch 
    
    // Document API
    // /{name}/data --> Args

    // ----
    // Background apply log + initial data => current starting point+version.
    
    // RDF Patch protocol and initial data.
    //   API? PatchLogHTTP : HTTP interface to a PAtychLog, not via DeltaLinkHTTP.
    //   Extract from DeltaLinkHTTP.
    
    // DeltaFuseki - sync on a timer.  

	// --autoregister, --no-register
    // RDFChangesHTTP.SuppressEmptyCommits.
    
    // Protect read in S_Data.
    
    // DeltaLinkHTTP ToDo about network handling
    // How does writing to disk work? Unify with collect->write
    
    // Eliminate use of Location and just use Path.
    
    // Streaming patches.
    // Binary patches.

    // ------------------------------------------------------------------------

    // -- javacc tokenizer:
    //   prefix name in ^^xsd:foo. (what does TokenizerText do? SubToken)
    //   See TestTokenizerJavaccTerms
    //     "0xXYZ"
    //     "''@lang- "

    // DatasetGraphBuffering
    // GraphBuffering
    // BufferingTupleSet<X>
}
