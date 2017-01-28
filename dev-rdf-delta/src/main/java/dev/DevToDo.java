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
    // Documentation
    //   Patch
    //   Protocol/delta
    
    // Examples.
    
    // Client side persistent counter and restart.
    
    // Tests assembler
    // Assembler to have a DeltaClient.
    
    // Tests - apply patch.
    // Plan for more?

    // HTTP API tests.
    //   Implement "not implemented"
    // Unregister
    // ** Operations happened when not registered.
    
    // Link tests.
    // TestDeltaServer in rdf-delta-server-http
    //   AbstractTestDeltaLink in rdf-delta-test
    //   ** TestRemoteLink
    // Test DatasetGraphBuffering
    
    // AbstractTestDeltaLink and AbstractTestDeltaConnection 
    
    // ------------------------------------------------------------------------
    // ClientId to "type" Id. 
    
    // "Design" protocol
    // See 
    //    AbstractTestDeltaLink.client_01
    //    AbstractTestDeltaConnection.client_01
    
    // -- History and parenthood.
    
    // TIO
    //   Token getType -> STRING, + getActualType "seen as".
    
    // -- javacc tokenizer:
    //   prefix name in ^^xsd:foo. (what does TokenizerText do? SubToken)
    //   See TestTokenizerJavaccTerms
    //     "0xXYZ"
    //     "''@lang- "

    //  Differences: token backtracking.
    //     0xXYZ is integer 0 (!)
    //  javacctokeizer has "push token" not "emit token".
    //    Compounds like RDFLiterals.

    // __ Others
    // Extract polling support to DeltaClient.

    // DatasetGraphBuffering
    // 
    // -- TIO
    //   Stream<Tuple<Token>>
    //     STRING1, STRING2 vs STRING
}
