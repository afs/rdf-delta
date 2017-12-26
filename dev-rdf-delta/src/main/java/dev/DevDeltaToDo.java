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
    // Examples:
    // Need to build up HA Fuseki. 
    /*
    DeltaExFuseki1_LogChanges.java [DONE]
    
    LocalDataset -> direct to Fuseki.
    LocalDataset -> to patch log server to Fuseki.
    
    DeltaExFuseki2_ChangesLocalDataset ???
    DeltaExFuseki2_ChangesLocalDataset ???
    
    
    DeltaExFuseki3_PatchOperation.java [DONE]

    DeltaExFuseki4_HighAvailability.java ???

    DeltaExLocal1_DatasetWithPatchLog.java [DONE]
    DeltaExLocal2_DatasetCollectPatch.java [DONE]
    
    DeltaExLocal3_PatchLogServer.
        Zone etc.
    
    */
    // Check site
    //   Use cases.
    //   Check issue #
    // Announce.

    
    // Merge registration of internal and external dsgs.
    // Assembler - where?
    // All tested!
    // Destination "file:" or local patch store?
    
    // documentation and examples of API (check web site)
    // DeltaClient:
    //    new data source.
    //    existing remote, new local.
    //    existing remote, existing local.
    //    existing local, deleted remote.
    
    // Remote test
    //   DataSourceDescription dsd = dClient.getLink().getDataSourceDescriptionByURI(baseURI);
    // Local test
    //   dClient.getZone().exists(dsRef)

    // ----
    // ** DatasetGraphChanges masks TDB ... DatasetGraph.exec(Op).
    
    // ** Fuseki
    // Fuseki - sync on begin(R,W)
    
    // ** "Get all patches x to y."

    // PatchStoreMgr.selectPatchStore - need to map dsId to PatchStore or PatchLog. 
    
    // F_BASE vs F_NAME

    // Test RDFChangesCancelOnNoChange
    // RDFChanges.cancel :: TestRDFChangesMiscImpls
    
    // Logging : detech by presence of logging.properties etc.
    // Need logging package?
    
    // Tests for "non-rows" RDFPatch 
    
    // Document API
    // /{name}/data --> Args

    // ----
    // Background apply log + initial data => current starting point+version.
    
    // RDF Patch protocol and initial data.
    //   API? PatchLogHTTP
    
    // DeltaFuseki - sync on a timer.  

    // ** Docs

	// --autoregister, --no-register
    // RDFChangesHTTP.SuppressEmptyCommits.
    
    // Protect read in S_Data.
    
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
