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
    // Document API

    // RDF Patch protocol and initial data.
    
    // DatasetGraphChanges masks TDB ... DatasetGraph.exec(Op).
    
    // DeltaFuseki - sync on a timer.  

    // DLink : name to id. function.
    //   And/or operations by name.
    
    // DatsetGraphBuffering
    
    // ** Persistent client-side data
    // ** Docs
    //    review delta.md
    //    Write up client stuff - sync'ing.
    // ** Initial data testing
    
    // S_Patch, S_Fetch. Gone? Invert and send to S_Log?
    //   POST patch -> ** Location: **
    //   "container/patch/{id}", "container/patch{version}"

    // PatchLog  concurrency.
    // PatchLog  Id for log separate from dsRef 

    // HTTP interface / Control interface :: clearer split.
    
    // PatchServer - client side tracking of the 
	//   Server lifecycle.
    //      ping (RPC1), RPC, HTTP / POST / GET / Non-retriable POST.
    //   Retry policy.
    //   op - ping - ping - op 

    // Registration token on S_Fetch
	// --autoregister, --no-register
    
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
    
    // Id.nil for "no previous".
    // Id for "any previous" (risky!)
    
    // Documentation
    //   Patch
    //   Protocol/delta
    // Version -> long?
    // Eliminate use of Location and just use Path.
    
    // Streaming patches.
    // Currently not because of DeltaLink.sendPatch takes a patch as argument. 

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
