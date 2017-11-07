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
    // PatchStoreMgr.selectPatchStore - need to map dsId to PatchStore or PatchLog. 
    
    
    // F_BASE vs F_NAME

    // Test RDFChangesCancelOnNoChange
    // RDFChanges.cancel :: TestRDFChangesMiscImpls
    
    // Fuseki patch receiver service.
    
    // RDFChangesCollector - add a Header Change and don't keep the map.
    // Doesn't work - begin->create,addtxnBegin then header. 
    // RDFChangesCollector.RECORD_HEADER = true;
    
    // Logging : detech by presence of logging.properties etc.
    // Need logging package?
    
    // Z rows: the total number of rows. or transactions -> can notice truncation.
    //    Checksum each txn?  Cheap checksum?
    //        Add "Z <checksum>" to RDFPatch?
    //          Simple # length in rows.
    // Add "segmentEnd" to  RDFChanges.
    //   Keep start-finish. (1) Reusable RDFChanges (2) trigger completed actions 
    
    // Tests for "non-rows" RDFPatch 
    
    // Register the patch as a RDFLanguage.
    
    // Document API
    // /{name}/data --> Args

    // Be able to switch transaction processing off external control and/or merge in-patch transaction files. 
    
    // ----
    // Background apply log + initial data => current starting point+version.
    
    // RDF Patch protocol and initial data.
    //   API? PatchLogHTTP
    
    // DatasetGraphChanges masks TDB ... DatasetGraph.exec(Op).
   
    // DeltaFuseki - sync on a timer.  

    // ** Docs

	// --autoregister, --no-register
    // RDFChangesHTTP.SuppressEmptyCommits.
    
    // Protect read in S_Data.
    
    // Cmds 
    // rm --server URL x1 x2 x3 ...
    // mk --server URL x1 x2 x3 ...
    // list -server=
    // resync
    // append (gets patch head and fills that in)

    // "Get all patches x to y."
    
    // DeltaLinkHTTP ToDo about network handling
    // How does writing to disk work? Unify with collect->write
    
    // Eliminate use of Location and just use Path.
    
    // Streaming patches.

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
